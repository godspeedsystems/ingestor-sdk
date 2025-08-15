/**
 * @fileoverview Comprehensive unit tests for DataSourceApiUtils and its underlying service-specific utils.
 * This file mocks all external dependencies (axios, googleapis, fs) and internal dependencies
 * to test the logic of each class in isolation.
 */

import { DataSourceApiUtils } from '../src/functions/webhook-api/data-source-api-utils';
import { GitApiUtils } from '../src/functions/webhook-api/git-api-utils';
import { GDriveApiUtils } from '../src/functions/webhook-api/gdrive-api-utils';
import { logger } from '@godspeedsystems/core';
import axios from 'axios';
import { google } from 'googleapis';
import * as fs from 'fs/promises';

// --- MOCKS SETUP ---

// Mock the core logger
jest.mock('@godspeedsystems/core', () => ({
    logger: {
        info: jest.fn(),
        error: jest.fn(),
        warn: jest.fn(),
        debug: jest.fn(),
    },
}));

// Mock the service-specific utility classes to test the dispatcher
jest.mock('../src/functions/webhook-api/git-api-utils');
jest.mock('../src/functions/webhook-api/gdrive-api-utils');

// Mock external dependencies for service-specific tests
jest.mock('axios');
jest.mock('googleapis');
jest.mock('fs/promises');

// --- TYPE CASTING FOR MOCKS ---
const MockedGitApiUtils = GitApiUtils as jest.Mocked<typeof GitApiUtils>;
const MockedGDriveApiUtils = GDriveApiUtils as jest.Mocked<typeof GDriveApiUtils>;
const mockedAxios = axios as jest.Mocked<typeof axios>;
const mockedGoogle = google as jest.Mocked<typeof google>;
const mockedFs = fs as jest.Mocked<typeof fs>;

// --- TEST SUITES ---

describe('API Utilities', () => {
    // Reset mocks before each test to ensure isolation
    beforeEach(() => {
        jest.clearAllMocks();
    });

    // --- DataSourceApiUtils: The Dispatcher ---
    describe('DataSourceApiUtils', () => {
        const credentials = { token: 'some-token' };

        describe('registerWebhook', () => {
            it('should dispatch to GitApiUtils for git-crawler', async () => {
                await DataSourceApiUtils.registerWebhook('git-crawler', 'owner/repo', 'url', 'id', credentials);
                expect(MockedGitApiUtils.registerWebhook).toHaveBeenCalledWith('owner/repo', 'url', 'id', credentials);
            });

            it('should dispatch to GDriveApiUtils for googledrive-crawler', async () => {
                await DataSourceApiUtils.registerWebhook('googledrive-crawler', 'folderId', 'url', 'id', credentials);
                expect(MockedGDriveApiUtils.registerWebhook).toHaveBeenCalledWith('folderId', 'url', 'id', credentials);
            });

            it('should handle case-insensitivity for pluginType', async () => {
                await DataSourceApiUtils.registerWebhook('Git-Crawler', 'owner/repo', 'url', 'id', credentials);
                expect(MockedGitApiUtils.registerWebhook).toHaveBeenCalledTimes(1);
            });

            it('should return an error for an unsupported plugin type', async () => {
                const result = await DataSourceApiUtils.registerWebhook('unsupported-service', 'id', 'url', 'id', credentials);
                expect(result).toEqual({ success: false, error: 'Unsupported plugin type: unsupported-service' });
            });
        });

        describe('deregisterWebhook', () => {
            it('should dispatch to GitApiUtils for git-crawler', async () => {
                await DataSourceApiUtils.deregisterWebhook('git-crawler', 'hookId', 'owner/repo', credentials);
                expect(MockedGitApiUtils.deregisterWebhook).toHaveBeenCalledWith('hookId', 'owner/repo', credentials);
            });

            it('should dispatch to GDriveApiUtils for googledrive-crawler', async () => {
                await DataSourceApiUtils.deregisterWebhook('googledrive-crawler', 'channelId', 'resourceId', credentials);
                expect(MockedGDriveApiUtils.deregisterWebhook).toHaveBeenCalledWith('channelId', 'resourceId', credentials);
            });

            it('should return an error for an unsupported plugin type', async () => {
                const result = await DataSourceApiUtils.deregisterWebhook('unsupported-service', 'id1', 'id2', credentials);
                expect(result).toEqual({ success: false, error: 'Unsupported plugin type: unsupported-service' });
            });
        });

        describe('verifyCredentials', () => {
            it('should dispatch to GitApiUtils for git', async () => {
                await DataSourceApiUtils.verifyCredentials('git', credentials);
                expect(MockedGitApiUtils.verifyCredentials).toHaveBeenCalledWith(credentials);
            });

            it('should dispatch to GDriveApiUtils for googledrive-crawler', async () => {
                await DataSourceApiUtils.verifyCredentials('googledrive-crawler', credentials);
                expect(MockedGDriveApiUtils.verifyCredentials).toHaveBeenCalledWith(credentials);
            });

            it('should return false for an unsupported plugin type', async () => {
                const result = await DataSourceApiUtils.verifyCredentials('unsupported-service', credentials);
                expect(result).toBe(false);
            });
        });
    });

    // --- GitApiUtils: GitHub Interactions ---
    describe('GitApiUtils (Actual Implementation)', () => {
        jest.unmock('../src/functions/webhook-api/git-api-utils');
        const ActualGitModule = require('../src/functions/webhook-api/git-api-utils');

        const accessToken = 'fake-token';
        const repoName = 'owner/repo';
        const webhookId = '12345';

        const mockAxiosInstance = {
            post: jest.fn(),
            delete: jest.fn(),
            get: jest.fn(),
        };
        mockedAxios.create.mockReturnValue(mockAxiosInstance as any);

        it('GitAPIClient constructor should throw an error if no access token is provided', () => {
            expect(() => new ActualGitModule.GitAPIClient(undefined)).toThrow('Access token is required for GitAPIClient.');
        });

        describe('registerWebhook', () => {
            it('should succeed and return the webhook ID', async () => {
                mockAxiosInstance.post.mockResolvedValue({ data: { id: webhookId } });
                const result = await ActualGitModule.GitApiUtils.registerWebhook(repoName, 'url', 'secret', accessToken);
                expect(result).toEqual({ success: true, externalId: webhookId });
            });

            it('should fail if the API call fails', async () => {
                mockAxiosInstance.post.mockRejectedValue(new Error('API Error'));
                const result = await ActualGitModule.GitApiUtils.registerWebhook(repoName, 'url', 'secret', accessToken);
                expect(result).toEqual({ success: false, error: 'API Error' });
            });

            it('should construct the correct webhook payload', async () => {
                const callbackUrl = 'https://example.com/callback';
                const secret = 'my-secret';
                const expectedPayload = {
                    name: 'web',
                    active: true,
                    events: ['push', 'pull_request'],
                    config: {
                        url: callbackUrl,
                        content_type: 'json',
                        secret: secret,
                    },
                };
                mockAxiosInstance.post.mockResolvedValue({ data: { id: webhookId } });
                await ActualGitModule.GitApiUtils.registerWebhook(repoName, callbackUrl, secret, accessToken);
                expect(mockAxiosInstance.post).toHaveBeenCalledWith(`/repos/${repoName}/hooks`, expectedPayload);
            });
        });

        describe('deregisterWebhook', () => {
            it('should succeed if the webhook exists and is deleted', async () => {
                mockAxiosInstance.get.mockResolvedValue({ data: [{ id: 12345 }] });
                mockAxiosInstance.delete.mockResolvedValue({});
                const result = await ActualGitModule.GitApiUtils.deregisterWebhook(repoName, webhookId, accessToken);
                expect(result).toEqual({ success: true });
            });

            it('should fail if required parameters are missing', async () => {
                const result = await ActualGitModule.GitApiUtils.deregisterWebhook(undefined, webhookId, accessToken);
               expect(result).toEqual({ success: false, error: 'Missing required parameters for webhook deregistration.' });
            });

            it('should fail if the webhook does not exist', async () => {
                mockAxiosInstance.get.mockResolvedValue({ data: [{ id: 999 }] });
                const result = await ActualGitModule.GitApiUtils.deregisterWebhook(repoName, webhookId, accessToken);
                expect(result.success).toBe(false);
            });

            it('should fail if the delete API call fails', async () => {
                mockAxiosInstance.get.mockResolvedValue({ data: [{ id: 12345 }] });
                mockAxiosInstance.delete.mockRejectedValue(new Error('Deletion failed'));
                const result = await ActualGitModule.GitApiUtils.deregisterWebhook(repoName, webhookId, accessToken);
                expect(result).toEqual({ success: false, error: 'Deletion failed' });
            });
        });

        describe('verifyCredentials', () => {
            it('should return true on a successful API call', async () => {
                mockAxiosInstance.get.mockResolvedValue({ data: { login: 'user' } });
                const result = await ActualGitModule.GitApiUtils.verifyCredentials(accessToken);
                expect(result).toBe(true);
            });

            it('should return false on a failed API call', async () => {
                mockAxiosInstance.get.mockRejectedValue(new Error('Invalid token'));
                const result = await ActualGitModule.GitApiUtils.verifyCredentials(accessToken);
                expect(result).toBe(false);
            });
        });
    });

    // --- GDriveApiUtils: Google Drive Interactions ---
    describe('GDriveApiUtils (Actual Implementation)', () => {
        jest.unmock('../src/functions/webhook-api/gdrive-api-utils');
        const ActualGDriveModule = require('../src/functions/webhook-api/gdrive-api-utils');

        const serviceAccountKey = {
            client_email: 'test@example.com',
            private_key: '-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n',
        };
        const serviceAccountConfig = { serviceAccountKey: JSON.stringify(serviceAccountKey) };
        const folderId = 'fake-folder-id';
        const channelToken = 'my-unique-channel-token';
        const callbackUrl = 'https://example.com/callback';

        const mockDriveClient = {
            changes: { getStartPageToken: jest.fn() },
            files: { watch: jest.fn() },
            channels: { stop: jest.fn() },
        };
        const mockJwtClient = { authorize: jest.fn() };
        (mockedGoogle.auth.JWT as unknown as jest.Mock) = jest.fn().mockImplementation(() => mockJwtClient);
        mockedGoogle.drive = jest.fn().mockReturnValue(mockDriveClient);

        describe('getAuthClient (private method tested via public methods)', () => {
            it('should successfully authenticate when using a serviceAccountKeyPath', async () => {
                mockedFs.readFile.mockResolvedValue(JSON.stringify(serviceAccountKey));
                mockJwtClient.authorize.mockResolvedValue(true);
                const result = await ActualGDriveModule.GDriveApiUtils.verifyCredentials({ serviceAccountKeyPath: '/good/path' });
                expect(result).toBe(true);
            });

            it('should fail if service account key path is used and file read fails', async () => {
                mockedFs.readFile.mockRejectedValue(new Error('File not found'));
                const result = await ActualGDriveModule.GDriveApiUtils.verifyCredentials({ serviceAccountKeyPath: '/bad/path' });
                expect(result).toBe(false);
            });

            // **NEW TEST** for invalid JSON in service account key string
            it('should fail if serviceAccountKey string is invalid JSON', async () => {
                const badConfig = { serviceAccountKey: '{"bad":"json"' };
                const result = await ActualGDriveModule.GDriveApiUtils.verifyCredentials(badConfig);
                expect(result).toBe(false);
                expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('Failed to load/parse service account key'), expect.any(Object));
            });

            // **NEW TEST** for key missing required properties
            it('should fail if service account key is missing client_email', async () => {
                const badKey = { ...serviceAccountKey, client_email: undefined };
                const badConfig = { serviceAccountKey: JSON.stringify(badKey) };
                const result = await ActualGDriveModule.GDriveApiUtils.verifyCredentials(badConfig);
                expect(result).toBe(false);
            });
        });

        describe('registerWebhook', () => {
            it('should succeed and return all necessary IDs and tokens', async () => {
                mockJwtClient.authorize.mockResolvedValue(true);
                mockDriveClient.changes.getStartPageToken.mockResolvedValue({ data: { startPageToken: 'token123' } });
                mockDriveClient.files.watch.mockResolvedValue({
                    data: { id: channelToken, resourceId: 'google-resource-id-abc' },
                });
                const result = await ActualGDriveModule.GDriveApiUtils.registerWebhook(folderId, callbackUrl, channelToken, serviceAccountConfig);
                expect(result).toEqual({
                    success: true,
                    secret: channelToken,
                    resourceId: folderId,
                    externalId: 'google-resource-id-abc',
                    startpageToken: 'token123',
                    nextPageToken: 'token123',
                });
            });

            it('should fail if watch API succeeds but returns no IDs', async () => {
                mockJwtClient.authorize.mockResolvedValue(true);
                mockDriveClient.changes.getStartPageToken.mockResolvedValue({ data: { startPageToken: 'token123' } });
                mockDriveClient.files.watch.mockResolvedValue({ data: {} });
                const result = await ActualGDriveModule.GDriveApiUtils.registerWebhook(folderId, callbackUrl, channelToken, serviceAccountConfig);
                expect(result).toEqual({ success: false, error: "API call succeeded, but no channel ID or resource ID was returned." });
            });

            it('should fail if getting start page token fails', async () => {
                mockJwtClient.authorize.mockResolvedValue(true);
                mockDriveClient.changes.getStartPageToken.mockResolvedValue({ data: {} });
                const result = await ActualGDriveModule.GDriveApiUtils.registerWebhook(folderId, callbackUrl, channelToken, serviceAccountConfig);
                expect(result).toEqual({ success: false, error: 'Failed to get starting page token.' });
            });

            it('should fail if authentication fails', async () => {
                mockJwtClient.authorize.mockRejectedValue(new Error('Auth failed'));
                const result = await ActualGDriveModule.GDriveApiUtils.registerWebhook(folderId, callbackUrl, channelToken, serviceAccountConfig);
                expect(result).toEqual({ success: false, error: 'Authentication failed: Auth failed' });
            });

            it('should construct the correct channel payload', async () => {
                const callbackUrl = 'https://example.com/callback';
                const channelToken = 'my-unique-channel-token';
                const expectedPayload = {
                    id: channelToken,
                    type: 'web_hook',
                    address: callbackUrl.trim(),
                };

                mockJwtClient.authorize.mockResolvedValue(true);
                mockDriveClient.changes.getStartPageToken.mockResolvedValue({ data: { startPageToken: 'token123' } });
                mockDriveClient.files.watch.mockResolvedValue({
                    data: { id: channelToken, resourceId: 'google-resource-id-abc' },
                });

                await ActualGDriveModule.GDriveApiUtils.registerWebhook(folderId, callbackUrl, channelToken, serviceAccountConfig);

                expect(mockDriveClient.files.watch).toHaveBeenCalledWith({
                    fileId: folderId,
                    requestBody: expectedPayload,
                });
            });
        });

        describe('deregisterWebhook', () => {
            it('should succeed when API call is successful', async () => {
                mockJwtClient.authorize.mockResolvedValue(true);
                mockDriveClient.channels.stop.mockResolvedValue({});
                const result = await ActualGDriveModule.GDriveApiUtils.deregisterWebhook('chan-id', 'res-id', serviceAccountConfig);
                expect(result).toEqual({ success: true });
            });

            it('should fail if the API call fails', async () => {
                mockJwtClient.authorize.mockResolvedValue(true);
                mockDriveClient.channels.stop.mockRejectedValue(new Error('Stop error'));
                const result = await ActualGDriveModule.GDriveApiUtils.deregisterWebhook('chan-id', 'res-id', serviceAccountConfig);
                expect(result).toEqual({ success: false, error: 'Stop error' });
            });
        });
    });
});
