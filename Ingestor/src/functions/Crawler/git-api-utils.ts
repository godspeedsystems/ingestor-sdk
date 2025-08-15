import axios, { AxiosInstance } from 'axios';
import { GSContext, GSDataSource, GSStatus } from "@godspeedsystems/core";
import { logger } from "@godspeedsystems/core";

/**
 * A dedicated client for making authenticated API calls to a Git provider like GitHub.
 */
export class GitAPIClient {
    private axiosClient: AxiosInstance;
    private baseUrl: string;

    constructor(accessToken: string) {
        if (!accessToken) {
            throw new Error('Access token is required for GitAPIClient.');
        }
        this.baseUrl = 'https://api.github.com';

        this.axiosClient = axios.create({
            baseURL: this.baseUrl,
            headers: {
                'Authorization': `token ${accessToken}`,
                'Content-Type': 'application/json',
            },
            timeout: 15000,
        });
    }

    /**
     * Creates a webhook for a given repository.
     */
    public async createWebhook(repoName: string, payload: any): Promise<any> {
        logger.info(`[GitAPIClient] Registering webhook for repo: ${repoName}`);
        try {
            const response = await this.axiosClient.post(`/repos/${repoName}/hooks`, payload);
            return response.data;
        } catch (error: any) {
            logger.error(`[GitAPIClient] Failed to create webhook for ${repoName}: ${error.message}`, {
                status: error.response?.status,
                data: error.response?.data,
            });
            throw error;
        }
    }

    /**
     * Deletes a webhook by its ID, first verifying its existence.
     */
    public async deleteWebhook(repoName: string, webhookId: string): Promise<void> {
        logger.info(`[GitAPIClient] Attempting to delete webhook with ID: ${webhookId} from repository: ${repoName}`);
        try {
            // To provide a clearer error message, first verify the webhook exists before trying to delete it.
            const allWebhooksResponse = await this.axiosClient.get(`/repos/${repoName}/hooks`);
            const webhookExists = allWebhooksResponse.data.some(
                (webhook: { id: number }) => webhook.id === Number(webhookId)
            );

            if (!webhookExists) {
                logger.warn(`[GitAPIClient] Webhook with ID ${webhookId} not found in the repository's hooks list.`);
                throw new Error(`Webhook with ID ${webhookId} not found. Status: 404`);
            }

            await this.axiosClient.delete(`/repos/${repoName}/hooks/${Number(webhookId)}`);
            logger.info(`[GitAPIClient] Successfully deleted webhook with ID: ${webhookId}`);
        } catch (error: any) {
            logger.error(`[GitAPIClient] Failed to delete webhook ${webhookId}: ${error.message}`, {
                status: error.response?.status,
                data: error.response?.data,
            });
            throw error;
        }
    }

    /**
     * Fetches the user profile to verify the provided access token.
     */
    public async getUserProfile(): Promise<any> {
        logger.info(`[GitAPIClient] Verifying token...`);
        try {
            const response = await this.axiosClient.get(`/user`);
            return response.data;
        } catch (error: any) {
            logger.error(`[GitAPIClient] Token verification failed: ${error.message}`, {
                status: error.response?.status,
                data: error.response?.data,
            });
            throw error;
        }
    }
}

export interface WebhookRegistrationResult {
    success: boolean;
    externalId?: string;
    error?: string;
}

export interface WebhookDeregistrationResult {
    success: boolean;
    error?: string;
}

/**
 * Provides high-level utility functions to manage Git webhooks.
 */
export class GitApiUtils {

    /**
     * Registers a new webhook with the Git hosting service.
     */
    public static async registerWebhook(
        externalResourceId: string,
        callbackUrl: string,
        secret: string,
        accessToken: string
    ): Promise<WebhookRegistrationResult> {
        logger.info(`[GitApiUtils] Starting webhook registration for repository: ${externalResourceId}`);
        try {
            const gitClient = new GitAPIClient(accessToken);
            const repoName = externalResourceId;

            const webhookPayload = {
                name: 'web',
                active: true,
                events: ['push', 'pull_request'],
                config: {
                    url: callbackUrl,
                    content_type: 'json',
                    secret: secret,
                },
            };
            const response = await gitClient.createWebhook(repoName, webhookPayload);

            logger.info(`[GitApiUtils] Successfully registered webhook with ID: ${response.id} for repository: ${externalResourceId}`);
            
            return { success: true, externalId: response.id.toString() }; // Ensure ID is a string
        } catch (error: any) {
            logger.error(`[GitApiUtils] Failed to register webhook for ${externalResourceId}: ${error.message}`);
            return { success: false, error: error.message };
        }
    }

    /**
     * De-registers a webhook from the Git hosting service.
     */
    public static async deregisterWebhook(
        externalResourceId: string, // e.g., 'owner/repo'
        webhookId: string,
        accessToken: string
    ): Promise<WebhookDeregistrationResult> {
        logger.info(`[GitApiUtils] Starting webhook deregistration for webhook ID: ${webhookId} on repository: ${externalResourceId}`);
        try {
            if (!externalResourceId || !webhookId || !accessToken) {
                return { success: false, error: 'Missing required parameters for webhook deregistration.' };
            }

            const gitClient = new GitAPIClient(accessToken);
            await gitClient.deleteWebhook(externalResourceId, webhookId);

            logger.info(`[GitApiUtils] Successfully deregistered GitHub webhook with ID: ${webhookId}`);
            return { success: true };
        } catch (error: any) {
            logger.error(`[GitApiUtils] Failed to deregister GitHub webhook ${webhookId}: ${error.message}`);
            return { success: false, error: error.message };
        }
    }

    /**
     * Verifies that the provided access token is valid.
     */
    public static async verifyCredentials(accessToken: string): Promise<boolean> {
        try {
            const gitClient = new GitAPIClient(accessToken);
            await gitClient.getUserProfile();
            return true;
        } catch (error) {
            return false;
        }
    }
}