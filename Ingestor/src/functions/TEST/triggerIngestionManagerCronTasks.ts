// \src\functions\TEST\triggerIngestionManagerCronTasks.ts

// Imports necessary modules from Godspeed core and the local final-test file.
import { GSContext, logger } from '@godspeedsystems/core';
import { globalIngestionManager } from './final-test' // Imports the singleton instance of the GlobalIngestionLifecycleManager

/**
 * Godspeed event handler for cron triggers.
 * This function is expected to be called by a Godspeed cron event source
 * (e.g., configured in events/cron_events.yaml).
 * It instructs the GlobalIngestionLifecycleManager to check and trigger
 * any scheduled cron tasks that are due.
 *
 * @param ctx The Godspeed context object, containing event details.
 * @returns A GSStatus indicating the result of triggering cron tasks.
 */
export default async function (ctx: GSContext) {
    // Log that a cron trigger has been received and the manager check is initiating.
    logger.info("--- Received cron trigger. Initiating comprehensive manager method check. ---");
    logger.info("Attempting to trigger all enabled cron tasks via GlobalIngestionLifecycleManager.");
    
    try {
        // Call the GlobalIngestionLifecycleManager's method to find and trigger due cron tasks.
        // The manager handles all the internal logic like checking schedules, fetching tasks from DB,
        // and invoking the crawlers.
        const status = await globalIngestionManager.triggerAllEnabledCronTasks(ctx);

        // Return the status received from the manager's operation.
        return status;
    } catch (error: any) {
        // Catch any unexpected errors during the triggering process and log them.
        // Return a failed GSStatus to indicate an issue.
        logger.error(`Error in triggerIngestionManagerCronTasks: ${error.message}`, { error });
        return { success: false, message: `Failed to trigger cron tasks: ${error.message}` };
    }
}