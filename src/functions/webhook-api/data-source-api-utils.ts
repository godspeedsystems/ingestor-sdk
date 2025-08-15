//C:\Users\SOHAM\Desktop\Crawler-sdk\Crawler-sdk\src\functions\webhook-api\data-source-api-utils.ts

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
 * Interface for a generic webhook registration result.
 */
export interface WebhookRegistrationResult {
    success: boolean;
    externalId?: string;
    startpageToken?: string;
    nextPageToken?:string
    error?: string;
}

/**
 * Interface for a generic webhook deregistration result.
 */
export interface WebhookDeregistrationResult {
    success: boolean;
    error?: string;
}

export class DataSourceApiUtils {

    /**
     * Dispatches the webhook registration request to the correct utility class.
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
     * Dispatches the webhook deregistration request to the correct utility class.
     */
    public static async deregisterWebhook(
        pluginType: string,
        webhookId:string,
        resourceId: string,
        credentials: any
    ): Promise<WebhookDeregistrationResult> {
        switch (pluginType.toLowerCase()) {
            case 'git-crawler':
                return GitApiUtils.deregisterWebhook(webhookId,resourceId, credentials);
            case 'googledrive-crawler':
                return GDriveApiUtils.deregisterWebhook(webhookId,resourceId, credentials);
            default:
                logger.error(`No deregistration handler for plugin type: ${pluginType}`);
                return { success: false, error: `Unsupported plugin type: ${pluginType}` };
        }
    }

    /**
     * Dispatches the credentials verification request to the correct utility class.
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
