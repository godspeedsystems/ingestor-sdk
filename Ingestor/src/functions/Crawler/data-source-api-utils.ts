import { GSContext, GSDataSource, GSStatus } from "@godspeedsystems/core";
import { logger } from "@godspeedsystems/core";
import {
    GitApiUtils,
    WebhookRegistrationResult as GitRegResult,
    WebhookDeregistrationResult as GitDeregResult
} from './git-api-utils';
import {
    GDriveApiUtils,
    WebhookRegistrationResult as GDriveRegResult,
    WebhookDeregistrationResult as GDriveDeregResult
} from './gdrive-api-utils';

/**
 * A generic result structure for webhook registration operations.
 */
export interface WebhookRegistrationResult {
    success: boolean;
    externalId?: string;
    startpageToken?: string;
    nextPageToken?: string
    error?: string;
}

/**
 * A generic result structure for webhook deregistration operations.
 */
export interface WebhookDeregistrationResult {
    success: boolean;
    error?: string;
}

/**
 * Acts as a facade or dispatcher for all data source API interactions.
 * This class centralizes logic and routes requests to the appropriate
 * service-specific utility (e.g., GitApiUtils, GDriveApiUtils).
 */
export class DataSourceApiUtils {

    /**
     * Registers a webhook by dispatching the request to the correct utility class based on pluginType.
     */
    public static async registerWebhook(
        pluginType: string,
        externalResourceId: string,
        callbackUrl: string,
        webhookId: string,
        credentials: any
    ): Promise<WebhookRegistrationResult> {
        switch (pluginType.toLowerCase()) {
            case 'git-crawler':
                return GitApiUtils.registerWebhook(
                    externalResourceId,
                    callbackUrl,
                    webhookId,
                    credentials
                );
            case 'googledrive-crawler':
                return GDriveApiUtils.registerWebhook(
                    externalResourceId,
                    callbackUrl,
                    webhookId,
                    credentials
                );
            default:
                logger.error(`No webhook registration handler for plugin type: ${pluginType}`);
                return { success: false, error: `Unsupported plugin type: ${pluginType}` };
        }
    }

    /**
     * Deregisters a webhook by dispatching the request to the correct utility class.
     */
    public static async deregisterWebhook(
        pluginType: string,
        webhookId: string,
        resourceId: string,
        credentials: any
    ): Promise<WebhookDeregistrationResult> {
        switch (pluginType.toLowerCase()) {
            case 'git-crawler':
                return GitApiUtils.deregisterWebhook(webhookId, resourceId, credentials);
            case 'googledrive-crawler':
                return GDriveApiUtils.deregisterWebhook(webhookId, resourceId, credentials);
            default:
                logger.error(`No deregistration handler for plugin type: ${pluginType}`);
                return { success: false, error: `Unsupported plugin type: ${pluginType}` };
        }
    }

    /**
     * Verifies credentials by dispatching the request to the correct utility class.
     */
    public static async verifyCredentials(
        pluginType: string,
        credentials: any
    ): Promise<boolean> {
        switch (pluginType.toLowerCase()) {
            case 'git':
                return GitApiUtils.verifyCredentials(credentials);
            case 'googledrive-crawler':
                return GDriveApiUtils.verifyCredentials(credentials);
            default:
                logger.error(`No credentials verification handler for plugin type: ${pluginType}`);
                return false;
        }
    }
}