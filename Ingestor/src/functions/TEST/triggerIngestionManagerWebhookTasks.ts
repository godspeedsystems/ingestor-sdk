// \src\functions\TEST\triggerIngestionManagerWebhookTasks.ts

import { GSContext, GSStatus, logger } from "@godspeedsystems/core";
import { globalIngestionManager } from './final-test' // Imports the singleton instance of the GlobalIngestionLifecycleManager

export default async function (ctx: GSContext): Promise<GSStatus> {
    logger.info("------------Received webhook event.-----------------------"); // Log webhook reception

    // Extract raw webhook payload, headers, and endpoint type from the Godspeed context.
    const webhookPayload = (ctx.inputs as any).data.body;
    const requestHeaders = (ctx.inputs as any).data.headers;
    const eventTypePath = (ctx.inputs as any).type; // Represents the endpoint ID (e.g., '/webhook/github/')

    try {
        // Delegate the webhook processing to the GlobalIngestionLifecycleManager.
        // This manager handles validation, task identification, and crawler invocation.
        const result = await globalIngestionManager.triggerWebhookTask(ctx, eventTypePath, webhookPayload, requestHeaders);

        // Check if the webhook processing failed or returned an invalid result.
        if (!result.success) { // Check result.success instead of !result
            logger.warn(`Failed to process webhook: ${result.message || 'Unknown error'}`, { result });
            return new GSStatus(false, 400, `Webhook validation failed: ${result.message || 'Unknown error'}`);
        }

        // --- Optional: Code for delayed task deletion as the scheduler is not using db so use these method for deregistering webhook(currently commented out) ---
        // This section demonstrates how tasks could be programmatically deleted after a delay.
        // It's commented out, so it does not execute during normal operation.
        // logger.warn("delete initiated.....")
        // function sleep(ms: number): Promise<void> {
        //     return new Promise(resolve => setTimeout(resolve, ms));
        // }
        // async function deleteWithDelay() {
        //     await sleep(30000); // wait for 30 seconds
        //     await globalIngestionManager.deleteTask('my-webhook-google-drive-crawl-task');
        //     const response = await globalIngestionManager.deleteTask('webhook-git-clone');
        //     console.log("Delete response:", response);
        // }
        // deleteWithDelay();
        // --- End of optional deletion code ---

        // Return a successful status indicating the webhook event was processed.
        return new GSStatus(true, 200, "Webhook event processed and ingestion triggered.", result);

    } catch (error: any) {
        // Catch any unexpected errors during webhook processing and return a failure status.
        logger.error(`Error while processing webhook: ${error.message}`, { error });
        return new GSStatus(false, 500, `Internal server error: ${error.message}`);
    }
}