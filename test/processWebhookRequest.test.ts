/**
 * @fileoverview Comprehensive unit tests for the processWebhookRequest function and its internal handlers.
 * This file uses the Jest testing framework to mock dependencies and validate all logic paths.
 */

import * as crypto from 'crypto';
import { processWebhookRequest } from '../src/functions/processWebhookRequest'; // Adjust the import path as needed
import { logger } from '@godspeedsystems/core';

// --- MOCKS ---

// Mock the logger to prevent actual logging during tests and to allow spying on calls.
jest.mock('@godspeedsystems/core', () => ({
    logger: {
        warn: jest.fn(),
        error: jest.fn(),
    },
}));

// Mock the entire crypto module
jest.mock('crypto');

// --- TEST SUITE ---

describe('Webhook Processing Logic', () => {
    // Cast the mocked crypto to its Jest Mock type to satisfy TypeScript
    const mockedCrypto = crypto as jest.Mocked<typeof crypto>;

    beforeEach(() => {
        // Clear all mocks before each test to ensure a clean state.
        jest.clearAllMocks();
    });

    /**
     * Tests for the main exported function: processWebhookRequest
     */
    describe('processWebhookRequest', () => {
        it('should return an error for an unsupported webhook service', () => {
            const result = processWebhookRequest('unsupported-service', {}, 'secret', '{}');
            expect(result).toEqual({
                isValid: false,
                error: 'No handler found for webhook service: unsupported-service',
                payload: undefined,
                externalResourceId: undefined,
                changeType: undefined,
            });
        });

        it('should return an error for an invalid body type', () => {
            const result = processWebhookRequest('git-crawler', {}, 'secret', 12345 as any); // Using a number as body
            expect(result).toEqual({
                isValid: false,
                error: 'Invalid webhook body type.',
            });
        });

        it('should correctly handle a stringified JSON body', () => {
            const body = JSON.stringify({ repository: { full_name: 'test/repo' } });
            const result = processWebhookRequest('git-crawler', { 'x-github-event': 'push' }, undefined, body);
            expect(result.isValid).toBe(true);
            expect(result.externalResourceId).toBe('https://github.com/test/repo');
        });

        it('should correctly handle a JSON object body', () => {
            const body = { repository: { full_name: 'test/repo' } };
            const result = processWebhookRequest('git-crawler', { 'x-github-event': 'push' }, undefined, body);
            expect(result.isValid).toBe(true);
            expect(result.externalResourceId).toBe('https://github.com/test/repo');
        });

        it('should correctly map a successful result to ProcessedWebhookResult', () => {
            const body = { repository: { full_name: 'test/repo' } };
            const result = processWebhookRequest('git-crawler', { 'x-github-event': 'push' }, undefined, body);
            expect(result).toEqual({
                isValid: true,
                payload: body,
                externalResourceId: 'https://github.com/test/repo',
                changeType: 'UPSERT',
                error: undefined,
            });
        });

        it('should set isValid to false if signature validation fails when a secret is provided', () => {
            const secret = 'my-secret';
            const body = JSON.stringify({ repository: { full_name: 'test/repo' } });
            const headers = {
                'x-github-event': 'push',
                'x-hub-signature-256': 'sha256=wrong_hash',
            };
            // Mock the HMAC result for the code under test
            const hmacMock = {
                update: jest.fn().mockReturnThis(),
                digest: jest.fn().mockReturnValue('correct_hash'),
            };
            mockedCrypto.createHmac.mockReturnValue(hmacMock as any);

            const result = processWebhookRequest('git-crawler', headers, secret, body);

            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Invalid webhook signature.');
        });

        it('should set isValid to true if signature validation is not required (no secret)', () => {
            const body = { repository: { full_name: 'test/repo' } };
            const result = processWebhookRequest('git-crawler', { 'x-github-event': 'push' }, undefined, body);
            expect(result.isValid).toBe(true);
            expect(result.error).toBeUndefined();
        });
    });

    /**
     * Tests for the internal Git webhook handler: handleGitWebhook
     */
    describe('handleGitWebhook', () => {
        const secret = 'a-super-secret-key';
        const validBody = JSON.stringify({
            repository: { full_name: 'owner/repo' },
            deleted: false,
        });

        it('should successfully validate a correct signature and process a push event', () => {
            // The hash the code will generate based on our mock
            const calculatedHash = 'mocked-correct-hash';
            const headers = {
                'x-github-event': 'push',
                'x-hub-signature-256': `sha256=${calculatedHash}`,
            };

            // Mock the crypto functions for this specific test
            const hmacMock = {
                update: jest.fn().mockReturnThis(),
                digest: jest.fn().mockReturnValue(calculatedHash),
            };
            mockedCrypto.createHmac.mockReturnValue(hmacMock as any);

            const result = processWebhookRequest('git-crawler', headers, secret, validBody);

            expect(result.isValid).toBe(true);
            expect(result.externalResourceId).toBe('https://github.com/owner/repo');
            expect(result.changeType).toBe('UPSERT');
            expect(result.error).toBeUndefined();
            expect(mockedCrypto.createHmac).toHaveBeenCalledWith('sha256', secret);
            expect(hmacMock.update).toHaveBeenCalledWith(validBody);
        });

        it('should identify a DELETE change type for a deleted push event', () => {
            const deleteBody = JSON.stringify({
                repository: { full_name: 'owner/repo' },
                deleted: true,
            });
            const result = processWebhookRequest('git-crawler', { 'x-github-event': 'push' }, undefined, deleteBody);
            expect(result.changeType).toBe('DELETE');
        });
        
        it('should identify an UPSERT change type for a pull_request event', () => {
            const prBody = JSON.stringify({
                repository: { full_name: 'owner/repo' },
                action: 'opened',
            });
            const result = processWebhookRequest('git-crawler', { 'x-github-event': 'pull_request' }, undefined, prBody);
            expect(result.changeType).toBe('UPSERT');
        });

        it('should return an error for an invalid JSON payload', () => {
            const result = processWebhookRequest('git-crawler', {}, secret, '{"bad json":,');
            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Invalid JSON payload.');
        });

        it('should return an error for a missing signature when a secret is provided', () => {
            const result = processWebhookRequest('git-crawler', { 'x-github-event': 'push' }, secret, validBody);
            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Webhook signature validation skipped or failed.');
            expect(logger.warn).toHaveBeenCalledWith('Git webhook: Missing X-Hub-Signature header for validation.');
        });

        it('should return an error for an unsupported signature algorithm', () => {
            const headers = { 'x-hub-signature': 'sha1=somehash' };
            const result = processWebhookRequest('git-crawler', headers, secret, validBody);
            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Unsupported signature algorithm.');
        });

        it('should return an error for an invalid signature', () => {
            const headers = { 'x-hub-signature-256': 'sha256=wronghash' };
            const hmacMock = {
                update: jest.fn().mockReturnThis(),
                digest: jest.fn().mockReturnValue('correct-hash'), // The code calculates the correct hash
            };
            mockedCrypto.createHmac.mockReturnValue(hmacMock as any);
            const result = processWebhookRequest('git-crawler', headers, secret, validBody);
            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Invalid webhook signature.');
        });

        it('should return an error if the repository full_name is missing', () => {
            const body = JSON.stringify({ repository: {} }); // Missing full_name
            const result = processWebhookRequest('git-crawler', { 'x-github-event': 'push' }, undefined, body);
            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Could not extract externalResourceId (repository full name).');
        });

        it('should return an error if the repository object is missing', () => {
            const body = JSON.stringify({}); // Missing repository
            const result = processWebhookRequest('git-crawler', { 'x-github-event': 'push' }, undefined, body);
            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Could not extract externalResourceId (repository full name).');
        });

        it('should handle push event with missing deleted property', () => {
            const body = JSON.stringify({ repository: { full_name: 'owner/repo' } }); // Missing deleted
            const result = processWebhookRequest('git-crawler', { 'x-github-event': 'push' }, undefined, body);
            expect(result.changeType).toBe('UPSERT');
        });
    });

    /**
     * Tests for the internal Google Drive webhook handler: handleGDriveWebhook
     */
    describe('handleGDriveWebhook', () => {
        const expectedToken = 'valid-channel-id';
        const folderId = '1a2b3c4d5e6f';
        const resourceId = 'some-resource-id';
        const validHeaders = {
            'x-goog-channel-id': expectedToken,
            'x-goog-resource-state': 'exists',
            'x-goog-resource-uri': `https://www.googleapis.com/drive/v3/files/${folderId}?alt=json`,
            'x-goog-resource-id': resourceId,
        };

        it('should successfully validate a request with a correct channel token', () => {
            const result = processWebhookRequest('googledrive-crawler', validHeaders, expectedToken, '');
            expect(result.isValid).toBe(true);
            expect(result.externalResourceId).toBe(folderId);
            expect(result.changeType).toBe('UPSERT');
            expect(result.error).toBeUndefined();
            expect(result.payload).toHaveProperty('channelResourceId', resourceId);
        });

        it('should succeed without a token if none is expected', () => {
            const result = processWebhookRequest('googledrive-crawler', validHeaders, undefined, '');
            expect(result.isValid).toBe(true);
            expect(result.externalResourceId).toBe(folderId);
            expect(logger.warn).toHaveBeenCalledWith('GDrive webhook: No expected token provided for validation.');
        });

        it('should return an error for a token mismatch', () => {
            const headers = { ...validHeaders, 'x-goog-channel-id': 'wrong-token' };
            const result = processWebhookRequest('googledrive-crawler', headers, expectedToken, '');
            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Invalid channel ID or token mismatch.');
        });
        
        it('should return an error for a missing channel ID when a token is expected', () => {
            const { 'x-goog-channel-id': _, ...headers } = validHeaders;
            const result = processWebhookRequest('googledrive-crawler', headers, expectedToken, '');
            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Invalid channel ID or token mismatch.');
        });

        it('should return an error if the resource URI is missing', () => {
            const { 'x-goog-resource-uri': _, ...headers } = validHeaders;
            const result = processWebhookRequest('googledrive-crawler', headers, expectedToken, '');
            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Could not extract actual Drive folder ID from x-goog-resource-uri.');
        });
        
        it('should correctly identify changeType "DELETE" for "trash" state', () => {
            const headers = { ...validHeaders, 'x-goog-resource-state': 'trash' };
            const result = processWebhookRequest('googledrive-crawler', headers, undefined, '');
            expect(result.changeType).toBe('DELETE');
        });

        it('should correctly identify changeType "UPSERT" for "add" state', () => {
            const headers = { ...validHeaders, 'x-goog-resource-state': 'add' };
            const result = processWebhookRequest('googledrive-crawler', headers, undefined, '');
            expect(result.changeType).toBe('UPSERT');
        });

        it('should return an error if the resource URI is malformed', () => {
            const headers = { ...validHeaders, 'x-goog-resource-uri': 'not a valid URI' };
            const result = processWebhookRequest('googledrive-crawler', headers, expectedToken, '');
            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Could not extract actual Drive folder ID from x-goog-resource-uri.');
        });

        it('should return an error if the resource URI does not contain a folder ID', () => {
            const headers = { ...validHeaders, 'x-goog-resource-uri': 'https://www.googleapis.com/drive/v3/files/?alt=json' };
            const result = processWebhookRequest('googledrive-crawler', headers, expectedToken, '');
            expect(result.isValid).toBe(false);
            expect(result.error).toBe('Could not extract actual Drive folder ID from x-goog-resource-uri.');
        });

        it('should correctly identify changeType "UPSERT" for "exists" state', () => {
            const headers = { ...validHeaders, 'x-goog-resource-state': 'exists' };
            const result = processWebhookRequest('googledrive-crawler', headers, undefined, '');
            expect(result.changeType).toBe('UPSERT');
        });

        it('should correctly identify changeType "DELETE" for "not_exists" state', () => {
            const headers = { ...validHeaders, 'x-goog-resource-state': 'not_exists' };
            const result = processWebhookRequest('googledrive-crawler', headers, undefined, '');
            expect(result.changeType).toBe('DELETE');
        });
    });
});
