import * as crypto from 'crypto';
import { logger } from '@godspeedsystems/core';
import { ProcessedWebhookResult } from '../Scheduler/interfaces';

/**
 * Defines the structured result returned by any internal webhook handler.
 */
export interface WebhookResult {
    success: boolean;
    externalResourceId?: string;
    rawPayload?: any;
    changeType?: 'UPSERT' | 'DELETE' | 'UNKNOWN';
    error?: string;
    signatureValidated?: boolean; 
}

/**
 * Handles incoming webhooks from Git providers like GitHub.
 * It validates the payload signature and extracts key information.
 */
const handleGitWebhook = (
    headers: Record<string, string>,
    body: string,
    secret?: string
): WebhookResult => {
    let payload: any;
    try {
        payload = JSON.parse(body);
    } catch (e: any) {
        return { success: false, error: 'Invalid JSON payload.', signatureValidated: false };
    }

    let signatureValidated = false;
    // Only perform signature validation if a secret has been configured for the webhook.
    if (secret) {
        try {
            const signature = headers['x-hub-signature-256'] || headers['x-hub-signature'];
            if (!signature) {
                logger.warn('Git webhook: Missing X-Hub-Signature header for validation.');
            } else {
                const [algorithm, hash] = signature.split('=');
                if (algorithm !== 'sha256') {
                    return { success: false, error: 'Unsupported signature algorithm.', signatureValidated: false };
                }
                // Validate the signature by creating an HMAC hash of the body and comparing it to the one in the header.
                const hmac = crypto.createHmac(algorithm, secret);
                hmac.update(body);
                const calculatedHash = hmac.digest('hex');
                if (calculatedHash !== hash) {
                    return { success: false, error: 'Invalid webhook signature.', signatureValidated: false };
                }
                signatureValidated = true;
            }
        } catch (e: any) {
            logger.error('Error during Git webhook signature validation:', e);
            return { success: false, error: 'Signature validation failed internally.', signatureValidated: false };
        }
    }

    const eventType = headers['x-github-event'];
    let changeType: 'UPSERT' | 'DELETE' | 'UNKNOWN' = 'UNKNOWN';
    if (eventType === 'push') {
        changeType = payload.deleted ? 'DELETE' : 'UPSERT';
    } else if (eventType === 'pull_request') {
         changeType = 'UPSERT';
    }

    const externalResourceId = payload.repository?.full_name;
    if (!externalResourceId) {
        return { success: false, error: 'Could not extract externalResourceId (repository full name).', signatureValidated: signatureValidated };
    }

    return {
        success: true,
        // The externalResourceId is converted to a full URL to maintain consistency with the task configuration.
        externalResourceId: `https://github.com/${externalResourceId}`,
        rawPayload: payload,
        changeType: changeType,
        signatureValidated: signatureValidated
    };
};

/**
 * Handles incoming push notifications from Google Drive.
 * It validates the channel token and extracts resource information from headers.
 */
const handleGDriveWebhook = (
    headers: Record<string, string>,
    body: string, 
    expectedToken?: string 
): WebhookResult => {
    let signatureValidated = false;
    // Validate the request by comparing the channel ID in the header to the expected token.
    if (expectedToken) { 
        const channelToken = headers['x-goog-channel-id'];
        if (!channelToken || channelToken !== expectedToken) {
            return { success: false, error: 'Invalid channel ID or token mismatch.', signatureValidated: false };
        }
        signatureValidated = true; 
    } else {
        logger.warn('GDrive webhook: No expected token provided for validation.');
    }

    // Google Drive notifications provide the resource ID (e.g., folder ID) in a URI within the headers.
    const resourceUri = headers['x-goog-resource-uri'];
    let externalResourceId: string | undefined;

    if (resourceUri) {
        try {
            // The URI format is like: https://www.googleapis.com/drive/v3/files/FOLDER_ID?alt=json
            const url = new URL(resourceUri);
            const pathSegments = url.pathname.split('/');
            // The resource ID is the last segment of the path.
            externalResourceId = pathSegments[pathSegments.length - 1];
        } catch (e: any) {
            logger.error(`Failed to parse x-goog-resource-uri: ${resourceUri}. Error: ${e.message}`);
        }
    }

    if (!externalResourceId) {
        return { success: false, error: 'Could not extract actual Drive folder ID from x-goog-resource-uri.', signatureValidated: signatureValidated };
    }

    const resourceState = headers['x-goog-resource-state'];
    let changeType: 'UPSERT' | 'DELETE' | 'UNKNOWN' = 'UNKNOWN';
    
    // Determine the type of change based on the state provided in the header.
    if (resourceState === 'exists' || resourceState === 'add' || resourceState === 'update') {
        changeType = 'UPSERT';
    } else if (resourceState === 'not_exists' || resourceState === 'trash') {
        changeType = 'DELETE';
    }

    // The body of a GDrive notification is often empty; the relevant data is in the headers.
    const rawPayload = {
        channelResourceId: headers['x-goog-resource-id'], 
        resourceState: resourceState,
        'x-goog-channel-id': headers['x-goog-channel-id'],
        'x-goog-message-number': headers['x-goog-message-number'],
        'x-goog-resource-state': headers['x-goog-resource-state'],
        'x-goog-resource-uri': headers['x-goog-resource-uri'],
    };

    return {
        success: true,
        externalResourceId: externalResourceId,
        rawPayload: rawPayload,
        changeType: changeType,
        signatureValidated: signatureValidated
    };
};

/**
 * A central function to process all incoming webhook requests. It authenticates,
 * normalizes, and dispatches the request to the appropriate handler.
 */
export const processWebhookRequest = (
    webhookService: string,
    headers: Record<string, string>,
    secret: string | undefined,
    body: any
): ProcessedWebhookResult => {
    let bodyString: string;
    
    // Ensure the request body is a string for consistent processing.
    if (typeof body === 'object' && body !== null) {
        bodyString = JSON.stringify(body);
    } else if (typeof body === 'string') {
        bodyString = body;
    } else {
        return { isValid: false, error: 'Invalid webhook body type.' };
    }

    let result: WebhookResult;

    // Dispatch to the correct internal handler based on the service type.
    if (webhookService === 'git-crawler') {
        result = handleGitWebhook(headers, bodyString, secret);
    } else if (webhookService === 'googledrive-crawler') {
        result = handleGDriveWebhook(headers, bodyString, secret);
    } else {
        result = { success: false, error: `No handler found for webhook service: ${webhookService}` };
    }

    // Map the internal result to the final, simplified ProcessedWebhookResult.
    return {
        isValid: (result.success && (secret ? result.signatureValidated : true)) as boolean,
        payload: result.rawPayload,
        externalResourceId: result.externalResourceId,
        changeType: result.changeType,
        error: result.error || (result.success && !result.signatureValidated && secret ? 'Webhook signature validation skipped or failed.' : undefined)
    };
};