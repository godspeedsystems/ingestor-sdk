// C:\Users\SOHAM\Desktop\crawler\test-crawler\src\functions\ingestion\GlobalIngestionLifecycleManager.ts

import { EventEmitter } from 'events';
import { CronExpressionParser } from 'cron-parser'; // Keep cron-parser for checking due tasks
import { v4 as uuidv4 } from 'uuid'; // Keep uuid for flowId generation
import {
    IGlobalIngestionLifecycleManager,
    IngestionTaskDefinition,
    GSDataSource,
    IngestionDataTransformer,
    IDestinationPlugin,
    CronTrigger,
    WebhookTrigger,
    IngestionTrigger,
    IngestionEvents,
    IngestionTaskStatus,
    IDatabaseService,
    WebhookRegistryEntry
} from './interfaces';
import { IngestionOrchestrator } from './orchestrator';
import { GSStatus, logger, GSContext } from '@godspeedsystems/core';
import { DataSource as GitCrawlerDataSource } from '../../datasources/types/git-crawler';
import { DataSource as GoogleDriveCrawlerDataSource } from '../../datasources/types/googledrive-crawler';
import { DataSource as HttpCrawlerDataSource } from '../../datasources/types/http-crawler';
import { passthroughTransformer } from './Transformers/passthrough-transformer';
import { htmlToPlaintextTransformer } from './Transformers/html-to-plaintext-transformer'; // Assuming this is the default for HTTP

import {ProcessedWebhookResult} from './interfaces'
import { DataSourceApiUtils } from '../Crawler/data-source-api-utils';
import { processWebhookRequest } from '../Crawler/processWebhookRequest'; // Verify this path and file
import crypto from 'crypto';

// --- InMemoryDatabaseService (for local testing) ---
// This class implements IDatabaseService using in-memory Maps.
// In a production environment, you would replace this with a real database implementation.
class InMemoryDatabaseService implements IDatabaseService {
    private tasks: Map<string, IngestionTaskDefinition> = new Map();
    private webhookRegistrations: Map<string, WebhookRegistryEntry> = new Map();

    async init(): Promise<void> {
        logger.info("InMemoryDatabaseService initialized.");
        // No actual initialization needed for in-memory maps
    }

    async getTask(taskId: string): Promise<IngestionTaskDefinition | undefined> {
        return this.tasks.get(taskId);
    }

    async saveTask(task: IngestionTaskDefinition): Promise<void> {
        this.tasks.set(task.id, task);
        logger.debug(`InMemoryDatabaseService: Task '${task.id}' saved.`);
    }

    async updateTask(taskId: string, updates: Partial<IngestionTaskDefinition>): Promise<void> {
        const existingTask = this.tasks.get(taskId);
        if (existingTask) {
            Object.assign(existingTask, updates);
            logger.debug(`InMemoryDatabaseService: Task '${taskId}' updated.`);
        } else {
            logger.warn(`InMemoryDatabaseService: Attempted to update non-existent task '${taskId}'.`);
        }
    }

    async deleteTask(taskId: string): Promise<void> {
        this.tasks.delete(taskId);
        logger.debug(`InMemoryDatabaseService: Task '${taskId}' deleted.`);
    }

    async listAllTasks(): Promise<IngestionTaskDefinition[]> {
        return Array.from(this.tasks.values());
    }

    async getWebhookRegistration(sourceIdentifier: string): Promise<WebhookRegistryEntry | undefined> {
        return this.webhookRegistrations.get(sourceIdentifier);
    }

    async saveWebhookRegistration(entry: WebhookRegistryEntry): Promise<void> {
        this.webhookRegistrations.set(entry.sourceIdentifier, entry);
        logger.debug(`InMemoryDatabaseService: Webhook registration for '${entry.sourceIdentifier}' saved.`);
    }

    async updateWebhookRegistration(sourceIdentifier: string, updates: Partial<WebhookRegistryEntry>): Promise<void> {
        const existingEntry = this.webhookRegistrations.get(sourceIdentifier);
        if (existingEntry) {
            Object.assign(existingEntry, updates);
            logger.debug(`InMemoryDatabaseService: Webhook registration for '${sourceIdentifier}' updated.`);
        } else {
            logger.warn(`InMemoryDatabaseService: Attempted to update non-existent webhook registration for '${sourceIdentifier}'.`);
        }
    }

    async deleteWebhookRegistration(sourceIdentifier: string): Promise<void> {
        this.webhookRegistrations.delete(sourceIdentifier);
        logger.debug(`InMemoryDatabaseService: Webhook registration for '${sourceIdentifier}' deleted.`);
    }
}

// --- GlobalIngestionLifecycleManager ---
interface DefaultCrawlerRegistryEntry {
    dataSource: new (...args: any[]) => GSDataSource;
    defaultTransformer: IngestionDataTransformer;
}

interface RegisteredPlugins {
    source: Map<string, { plugin: new (...args: any[]) => GSDataSource; transformer: IngestionDataTransformer }>;
    destination: Map<string, new (...args: any[]) => IDestinationPlugin>;
}

/**
 * GlobalIngestionLifecycleManager manages the scheduling and execution of all ingestion tasks.
 * It now integrates with a database service for persistence and handles webhook registration/deregistration.
 */
export class GlobalIngestionLifecycleManager extends EventEmitter implements IGlobalIngestionLifecycleManager {
    private registeredPlugins: RegisteredPlugins = { source: new Map(), destination: new Map() };
    private eventBus: EventEmitter = new EventEmitter();
    private dbService: IDatabaseService; // Now uses the IDatabaseService interface

    private static instance: GlobalIngestionLifecycleManager;

    private constructor(dbService?: IDatabaseService) {
        super();
        this.dbService = dbService || new InMemoryDatabaseService(); // Default to in-memory for local testing
        this.eventBus.on(IngestionEvents.TASK_COMPLETED, this.onTaskCompleted.bind(this));
        this.eventBus.on(IngestionEvents.TASK_FAILED, this.onTaskFailed.bind(this));
    }

      // FIX: Define the default crawler registry within the manager's scope
    private static readonly _defaultCrawlerRegistry: Record<string, DefaultCrawlerRegistryEntry> = {
        'git-crawler': { dataSource: GitCrawlerDataSource, defaultTransformer: passthroughTransformer },
        'googledrive-crawler': { dataSource: GoogleDriveCrawlerDataSource, defaultTransformer: passthroughTransformer },
        'http-crawler': { dataSource: HttpCrawlerDataSource, defaultTransformer: htmlToPlaintextTransformer },
        // Add other default crawlers here as needed
    };

    public static getInstance(dbService?: IDatabaseService): GlobalIngestionLifecycleManager {
        if (!GlobalIngestionLifecycleManager.instance) {
            GlobalIngestionLifecycleManager.instance = new GlobalIngestionLifecycleManager(dbService);
        } else if (dbService && GlobalIngestionLifecycleManager.instance.dbService instanceof InMemoryDatabaseService) {
            // Allow injecting a real DB service if currently using in-memory
            GlobalIngestionLifecycleManager.instance.setDatabaseService(dbService);
        }
        return GlobalIngestionLifecycleManager.instance;
    }

    public setDatabaseService(dbService: IDatabaseService): void {
        this.dbService = dbService;
        logger.info("GlobalIngestionLifecycleManager: Database service updated.");
    }

    async init(): Promise<void> {
        logger.info("GlobalIngestionLifecycleManager initializing...");
        await this.dbService.init(); // Initialize the underlying database service
        logger.info("GlobalIngestionLifecycleManager initialized.");
    }

    async start(): Promise<void> {
        logger.info("GlobalIngestionLifecycleManager starting...");
        // Re-schedule all enabled tasks from the database on start-up
        const allTasks = await this.dbService.listAllTasks();
        for (const task of allTasks) {
            if (task.enabled) {
                // For cron tasks, we only log that they are configured for Godspeed cron.
                // The actual triggering will happen via a Godspeed cron event calling triggerAllEnabledCronTasks.
                if (task.trigger.type === 'cron') {
                    logger.info(`Task '${task.id}' is configured for Godspeed Cron trigger "${(task.trigger as CronTrigger).expression}". Ensure a Godspeed cron event is set up to call triggerAllEnabledCronTasks().`);
                }
                // Webhooks are assumed to persist externally, but we ensure our registry is consistent
                // and potentially re-register if needed (e.g., if external webhook was lost)
                if (task.trigger.type === 'webhook') {
                    await this.registerWebhook(task); // Re-register or confirm existing
                }
            }
        }
        logger.info("GlobalIngestionLifecycleManager started. All enabled tasks configured.");
    }

    async stop(): Promise<void> {
        logger.info("GlobalIngestionLifecycleManager stopping. Clearing internal states.");
        // No cron jobs to destroy here as they are managed externally by Godspeed.
        // Webhooks are assumed to persist externally, so no mass deregistration here.
        // Individual deregistration happens on task deletion/disabling.
    }

    public getEventBus(): EventEmitter {
        return this.eventBus;
    }



   

    registerSource(pluginType: string, sourcePlugin: new (...args: any[]) => GSDataSource, transformer: IngestionDataTransformer): void {
        this.registeredPlugins.source.set(pluginType, { plugin: sourcePlugin, transformer });
        logger.info(`Source plugin '${pluginType}' registered.`);
    }

    registerDestination(pluginType: string, destinationPlugin: new (...args: any[]) => IDestinationPlugin): void {
        this.registeredPlugins.destination.set(pluginType, destinationPlugin);
        logger.info(`Destination plugin '${pluginType}' registered.`);
    }

     /**
     * Allows registering multiple default data sources by providing an array of their names.
     * This method looks up the DataSource class and default transformer from an internal registry.
     * @param crawlerTypes An array of string names for the crawler types to register (e.g., ['git-crawler', 'googledrive-crawler']).
     */
    public async sources(crawlerTypes: string[]): Promise<void> {
        logger.info("Registering default data sources from array...");
        for (const type of crawlerTypes) {
            const entry = GlobalIngestionLifecycleManager._defaultCrawlerRegistry[type];
            if (entry) {
                this.registerSource(type, entry.dataSource, entry.defaultTransformer);
                logger.info(`Registered '${type}' source with its default transformer.`);
            } else {
                logger.warn(`Attempted to register unknown crawler type: '${type}'. Skipping.`);
            }
        }
    }

    async scheduleTask(taskDefinition: IngestionTaskDefinition): Promise<GSStatus> {
        const taskId: string = taskDefinition.id || uuidv4();
        if (await this.dbService.getTask(taskId)) {
            logger.warn(`Task '${taskId}' already exists. Use updateTask to modify.`);
            return new GSStatus(false, 409, `Task '${taskId}' already exists.`);
        }

        // FIX: Removed JSON.parse(JSON.stringify()) for InMemoryDatabaseService as it's not needed
        // and might be causing subtle issues. Directly use the taskDefinition object or a shallow copy.
        const taskToSave: IngestionTaskDefinition = {
            ...taskDefinition, // Create a new object to avoid direct mutation of the input
            id: taskId,
            currentStatus: IngestionTaskStatus.SCHEDULED,
            lastRun: undefined,
            lastRunStatus: undefined,
        };

        await this.dbService.saveTask(taskToSave);
        this.eventBus.emit(IngestionEvents.TASK_SCHEDULED, taskToSave);
        logger.info(`Task '${taskToSave.name}' (${taskToSave.id}) scheduled.`);

        if (taskToSave.enabled) {
            const trigger = taskToSave.trigger as IngestionTrigger;
            if (trigger.type === 'cron') {
                logger.info(`Task '${taskToSave.id}' is configured for Godspeed Cron trigger "${(trigger as CronTrigger).expression}".`);
            } else if (trigger.type === 'webhook') {
                const registrationStatus = await this.registerWebhook(taskToSave);
                if (!registrationStatus.success) {
                    logger.error(`Failed to register webhook for task ${taskToSave.id}. Task might not be active.`);
                    await this.dbService.updateTask(taskToSave.id, { currentStatus: IngestionTaskStatus.FAILED, lastRunStatus: registrationStatus });
                    return registrationStatus;
                }
            }
        }
        return new GSStatus(true, 200, "Task scheduled successfully.");
    }
    
    async updateTask(taskId: string, updates: Partial<IngestionTaskDefinition>): Promise<GSStatus> {
        const existingTask = await this.dbService.getTask(taskId);
        if (!existingTask) {
            return new GSStatus(false, 404, `Task with ID ${taskId} not found.`);
        }

        const oldTask = JSON.parse(JSON.stringify(existingTask)) as IngestionTaskDefinition;
        const updatedTask = { ...existingTask, ...updates };

        await this.dbService.updateTask(taskId, updatedTask);
        this.eventBus.emit(IngestionEvents.TASK_UPDATED, updatedTask);
        logger.info(`Task '${taskId}' updated.`);

        // Handle webhook changes
        if (oldTask.trigger.type === 'webhook' || updatedTask.trigger.type === 'webhook') {
            if (oldTask.trigger.type === 'webhook' && updatedTask.trigger.type !== 'webhook') {
                await this.deregisterWebhook(oldTask.id);
            } else if (updatedTask.trigger.type === 'webhook') {
                const oldWebhookTrigger = oldTask.trigger as WebhookTrigger;
                const newWebhookTrigger = updatedTask.trigger as WebhookTrigger;

                const oldSourceIdentifier = this.getSourceIdentifier(oldTask.source.pluginType, oldTask.source.config);
                const newSourceIdentifier = this.getSourceIdentifier(updatedTask.source.pluginType, updatedTask.source.config);

                if (oldSourceIdentifier !== newSourceIdentifier || !newWebhookTrigger.externalWebhookId) {
                    if (oldSourceIdentifier) {
                        await this.deregisterWebhook(oldTask.id);
                    }
                    if (updatedTask.enabled) {
                        await this.registerWebhook(updatedTask);
                    }
                } else if (updates.enabled === false) {
                    await this.deregisterWebhook(taskId);
                } else if (updates.enabled === true && !newWebhookTrigger.externalWebhookId) {
                    await this.registerWebhook(updatedTask);
                }
            }
        }
        
        return new GSStatus(true, 200, "Task updated successfully.");
    }

    async enableTask(taskId: string): Promise<GSStatus> {
        const task = await this.dbService.getTask(taskId);
        if (!task) return new GSStatus(false, 404, `Task with ID ${taskId} not found.`);
        if (task.enabled) return new GSStatus(true, 200, "Task is already enabled.");

        const updates = { enabled: true };
        return this.updateTask(taskId, updates);
    }
    
    async disableTask(taskId: string): Promise<GSStatus> {
        const task = await this.dbService.getTask(taskId);
        if (!task) return new GSStatus(false, 404, `Task with ID ${taskId} not found.`);
        if (!task.enabled) return new GSStatus(true, 200, "Task is already disabled.");

        const updates = { enabled: false };
        return this.updateTask(taskId, updates);
    }
    
    async deleteTask(taskId: string): Promise<GSStatus> {
        const task = await this.dbService.getTask(taskId);
        if (!task) return new GSStatus(false, 404, `Task with ID ${taskId} not found.`);

        // Deregister webhook if this was the last task using it.
        const trigger = task.trigger as IngestionTrigger;
        let res
        if (trigger.type === 'webhook') {
            res = await this.deregisterWebhook(taskId);
        }
        if(!res?.success){
            return new GSStatus(false, 403, "error in deleting task");
        } 
        await this.dbService.deleteTask(taskId);
        this.eventBus.emit(IngestionEvents.TASK_DELETED, taskId);
        logger.info(`Task '${taskId}' deleted.`);

        return new GSStatus(true, 200, "Task deleted successfully.");
    }

    async getTask(taskId: string): Promise<IngestionTaskDefinition | undefined> {
        return this.dbService.getTask(taskId);
    }

    async listTasks(): Promise<IngestionTaskDefinition[]> {
        return this.dbService.listAllTasks();
    }

    async triggerManualTask(ctx: GSContext, taskId: string, initialPayload?: any): Promise<GSStatus> {
        const task = await this.dbService.getTask(taskId);
        if (!task) {
            const msg = `Task with ID ${taskId} not found.`;
            logger.error(msg);
            return new GSStatus(false, 404, msg);
        }
        if (!task.enabled) {
            const msg = `Task with ID ${taskId} is disabled and cannot be triggered manually.`;
            logger.warn(msg);
            return new GSStatus(false, 403, msg);
        }

        const sourceIdentifier = this.getSourceIdentifier(task.source.pluginType, task.source.config);
        if (sourceIdentifier) {
            const webhookEntry = await this.dbService.getWebhookRegistration(sourceIdentifier);
            if (webhookEntry) {
                initialPayload = {
                    ...initialPayload,
                    startPageToken: webhookEntry.startPageToken,
                    nextPageToken: webhookEntry.nextPageToken,
                    otherCrawlerSpecificTokens: webhookEntry.otherCrawlerSpecificTokens
                };
            }
        }

        return this.runOrchestrator(ctx, task, initialPayload);
    }

    async triggerWebhookTask(ctx: GSContext, endpointId: string, rawRequest: any, requestHeaders: any): Promise<GSStatus> {
        logger.info(`Webhook received for endpoint ID: ${endpointId}.`);
        
        // 1. Get all enabled webhook tasks for this endpoint (initial broad filter)
        const allWebhookTasksForEndpoint = await this.dbService.listAllTasks();
        const tasksMatchingEndpoint = allWebhookTasksForEndpoint.filter(
            t => t.enabled && t.trigger.type === 'webhook' && (t.trigger as WebhookTrigger).endpointId === endpointId
        );
         logger.info(`tasksMatchingEndpoint.length:${tasksMatchingEndpoint.length}`)
        if (tasksMatchingEndpoint.length === 0) {
            const msg = `(from trg webhook)No enabled tasks found for webhook endpoint ID: ${endpointId}.`;
            logger.warn(msg);
            return new GSStatus(false, 404, msg);
        }

        let firstStatus: GSStatus | undefined;
        let initialProcessedResult: ProcessedWebhookResult;
        let finalProcessedResult: ProcessedWebhookResult;

        try {
            // Determine the webhook service type from the first matching task
            const webhookService = tasksMatchingEndpoint[0].source.pluginType;

            // 2. Preliminary processing to extract externalResourceId and payload (without secret validation yet)
            
            initialProcessedResult = processWebhookRequest(webhookService, requestHeaders, undefined, rawRequest);

            if (!initialProcessedResult.isValid) {
                const msg = initialProcessedResult.error || `Preliminary webhook processing failed for endpoint ${endpointId}.`;
                logger.error(msg, { payload: initialProcessedResult.payload });
                return new GSStatus(false, 400, msg);
            }
            
            if (!initialProcessedResult.externalResourceId) {
                const msg = `Webhook processed successfully but could not extract externalResourceId for endpoint ${endpointId}.`;
                logger.error(msg, { payload: initialProcessedResult.payload });
                return new GSStatus(false, 400, msg);
            }

            // 3. Find the specific WebhookRegistryEntry using the extracted externalResourceId
           // logger.debug(`initialProcessedResult.externalResourceId:${initialProcessedResult.externalResourceId}`)
            const webhookEntry = await this.dbService.getWebhookRegistration(initialProcessedResult.externalResourceId);

            if (!webhookEntry) {
                const msg = `No webhook registration found for resource '${initialProcessedResult.externalResourceId}' linked to endpoint '${endpointId}'.`;
                logger.warn(msg);
                // It's a valid webhook, just no task configured for this specific resource.
                return new GSStatus(true, 200, msg); 
            }

            // 4. Perform signature validation using the correct secret from the registry entry
            finalProcessedResult = processWebhookRequest(webhookService, requestHeaders, webhookEntry.secret, rawRequest);

            if (!finalProcessedResult.isValid) {
                const msg = finalProcessedResult.error || "Webhook request signature validation failed.";
                logger.error(msg);
                return new GSStatus(false, 401, msg);
            }

            // Ensure the resource ID is still consistent after full validation (should be)
            if (finalProcessedResult.externalResourceId !== initialProcessedResult.externalResourceId) {
                const msg = `Resource ID mismatch after full webhook validation. Initial: ${initialProcessedResult.externalResourceId}, Final: ${finalProcessedResult.externalResourceId}`;
                logger.error(msg);
                return new GSStatus(false, 500, msg);
            }

            // 5. Filter tasks based on the task IDs registered with this specific webhookEntry
            const tasksToTriggerForResource = tasksMatchingEndpoint.filter(task => 
                webhookEntry.registeredTasks.includes(task.id)
            );

            if (tasksToTriggerForResource.length === 0) {
                const msg = `Webhook registration for resource '${initialProcessedResult.externalResourceId}' exists, but no enabled tasks are currently linked to it.`;
                logger.warn(msg);
                return new GSStatus(true, 200, msg);
            }

            // 6. Iterate and trigger only the relevant tasks
            for (const task of tasksToTriggerForResource) {
                // Re-fetch task to ensure latest state, though webhookEntry.registeredTasks should be authoritative
                const taskToExecute = await this.dbService.getTask(task.id);
                if (!taskToExecute) {
                    logger.warn(`Task '${task.id}' found in webhook registry but not in DB. Skipping.`);
                    continue;
                }

                const sourceIdentifier = this.getSourceIdentifier(taskToExecute.source.pluginType, taskToExecute.source.config);
                let currentWebhookState: WebhookRegistryEntry | undefined;
                if (sourceIdentifier) {
                    currentWebhookState = await this.dbService.getWebhookRegistration(sourceIdentifier);
                }

                const initialPayload = {
                    taskDefinition: taskToExecute, // Use the re-fetched task
                    webhookPayload: finalProcessedResult.payload, // Use the fully processed payload
                    externalResourceId: finalProcessedResult.externalResourceId, // Pass the extracted resource ID
                    changeType: finalProcessedResult.changeType, // Pass the extracted change type
                    startPageToken: currentWebhookState?.startPageToken,
                    nextPageToken: currentWebhookState?.nextPageToken,
                    otherCrawlerSpecificTokens: currentWebhookState?.otherCrawlerSpecificTokens
                };
                const status = await this.runOrchestrator(ctx, taskToExecute, initialPayload);
                if (!firstStatus) {
                    firstStatus = status; // Capture the status of the first task
                }
            }
            
            return firstStatus || new GSStatus(false, 500, "Webhook could not be triggered due to an unknown error.");

        } catch (err: any) {
            const msg = `Error processing webhook payload: ${err.message}`;
            logger.error(msg, { error: err });
            return new GSStatus(false, 500, msg);
        }
    }
    
    
    /**
     * This method is designed to be called by a Godspeed cron event.
     * It checks all enabled cron tasks and triggers those that are due.
     * @param ctx The Godspeed context provided by the cron event.
     * @returns A GSStatus indicating the result of triggering tasks.
     */
    public async triggerAllEnabledCronTasks(ctx: GSContext): Promise<GSStatus> {
        logger.info("Manager received command to trigger all enabled cron tasks. Checking due tasks...");

        // Use ctx.event?.time for 'now' to align with Godspeed's event timestamp, with fallback
        const now = new Date((ctx as any).event?.time || new Date().toISOString());
        const results: { taskId: string; status: GSStatus }[] = [];
        let tasksDueCount = 0;

        const allTasks = await this.dbService.listAllTasks(); // Fetch all tasks from DB
        for (const task of allTasks) {
            logger.debug(`Checking task: ${task.id}, enabled: ${task.enabled}, trigger type: ${task.trigger.type}`);

            if (task.enabled && task.trigger.type === 'cron') {
                const cronTrigger = task.trigger as CronTrigger;
                try {
                    const interval = CronExpressionParser.parse(cronTrigger.expression, { currentDate: now });
                    const previousRunTime = interval.prev().toDate(); // Last scheduled time before or at 'now'

                    // Define a robust window (e.g., 65 seconds) to account for slight delays in cron execution
                    const sixtyFiveSecondsAgo = new Date(now.getTime() - (65 * 1000));

                    // Condition:
                    // 1. previousRunTime must be after the 'sixtyFiveSecondsAgo' mark (it's recent)
                    // 2. previousRunTime must be at or before 'now' (it's not in the future)
                    // 3. AND (crucially) the task's lastRun must be undefined (never run)
                    //    OR the task's lastRun must be older than this specific previousRunTime.
                    //    This prevents re-running a task for the same scheduled interval if the trigger fires multiple times.
                    if (previousRunTime > sixtyFiveSecondsAgo && previousRunTime <= now &&
                        (!task.lastRun || task.lastRun < previousRunTime)) {

                        logger.info(`Executing cron-triggered task: ${task.id} (expression: ${cronTrigger.expression}, last due: ${previousRunTime.toISOString()}).`);
                        tasksDueCount++;

                        // Enrich initialPayload with latest tokens from webhook registry if applicable
                        const sourceIdentifier = this.getSourceIdentifier(task.source.pluginType, task.source.config);
                        let initialPayload: any = {};
                        if (sourceIdentifier) {
                            const webhookEntry = await this.dbService.getWebhookRegistration(sourceIdentifier);
                            if (webhookEntry) {
                                initialPayload = {
                                    startPageToken: webhookEntry.startPageToken,
                                    nextPageToken: webhookEntry.nextPageToken,
                                    otherCrawlerSpecificTokens: webhookEntry.otherCrawlerSpecificTokens
                                };
                            }
                        }
                        initialPayload = {taskDefinition: task,webhookPayload: "",}
                        const status = await this.runOrchestrator(ctx, task, initialPayload);
                        results.push({ taskId: task.id, status });
                    } else {
                        logger.debug(`Task '${task.id}' (cron: ${cronTrigger.expression}) not due. ` +
                                      `prevRun: ${previousRunTime.toISOString()}, ` +
                                      `now: ${now.toISOString()}, ` +
                                      `sixtyFiveSecondsAgo: ${sixtyFiveSecondsAgo.toISOString()}. ` +
                                      `lastRun: ${task.lastRun ? task.lastRun.toISOString() : 'never'}.`);
                    }
                } catch (error: any) {
                    logger.error(`Error parsing cron expression for task '${task.id}': ${cronTrigger.expression}. Error: ${error.message}`);
                    results.push({ taskId: task.id, status: { success: false, code: 500, message: `Cron expression parse error: ${error.message}` } });
                }
            }
        }

        if (tasksDueCount === 0) {
            logger.info("No enabled cron tasks were due at this time.");
            return new GSStatus(true, 200, "No enabled cron tasks were due.");
        }

        const successful = results.filter(r => r.status.success).length;
        const failed = results.length - successful;
        if (failed > 0) {
            return new GSStatus(false, 500, `Cron triggered ${tasksDueCount} tasks. ${successful} succeeded, ${failed} failed.`, { data: results });
        }
        return new GSStatus(true, 200, `Successfully triggered ${successful} cron tasks.`, { data: results });
    }

    private async runOrchestrator(ctx: GSContext, taskDefinition: IngestionTaskDefinition, initialPayload?: any): Promise<GSStatus> {
        logger.info(`Running orchestrator for task '${taskDefinition.id}'`);
        this.eventBus.emit(IngestionEvents.TASK_TRIGGERED, taskDefinition.id);
        
        await this.dbService.updateTask(taskDefinition.id, { currentStatus: IngestionTaskStatus.RUNNING });
        
        const sourceEntry = this.registeredPlugins.source.get(taskDefinition.source.pluginType);
        if (!sourceEntry) {
            const errorMessage = `Orchestrator failed: Source plugin '${taskDefinition.source.pluginType}' not found for task '${taskDefinition.id}'`;
            logger.error(errorMessage);
            const status = new GSStatus(false, 500, errorMessage);
            this.eventBus.emit(IngestionEvents.TASK_FAILED, taskDefinition.id, status);
            await this.dbService.updateTask(taskDefinition.id, { currentStatus: IngestionTaskStatus.FAILED, lastRunStatus: status });
            return status;
        }

        const destinationPlugin = taskDefinition.destination ? this.registeredPlugins.destination.get(taskDefinition.destination.pluginType) : undefined;
        let destinationInstance: IDestinationPlugin | undefined;

        if (destinationPlugin && taskDefinition.destination) {
            destinationInstance = new destinationPlugin(); // No config in constructor for IDestinationPlugin
            await destinationInstance.init(taskDefinition.destination.config);
        }

        const sourceInstance = new sourceEntry.plugin({ config: taskDefinition.source.config }); // Pass config as an object
        const orchestrator = new IngestionOrchestrator(
            sourceInstance,
            sourceEntry.transformer,
            destinationInstance,
            this.eventBus,
            taskDefinition.id
        );
        
       
      
        const finalStatus = await orchestrator.executeTask(ctx, initialPayload);

        if (finalStatus.success) {
            this.eventBus.emit(IngestionEvents.TASK_COMPLETED, taskDefinition.id, finalStatus);
            await this.dbService.updateTask(taskDefinition.id, { currentStatus: IngestionTaskStatus.COMPLETED, lastRun: new Date(), lastRunStatus: finalStatus });
        } else {
            this.eventBus.emit(IngestionEvents.TASK_FAILED, taskDefinition.id, finalStatus);
            await this.dbService.updateTask(taskDefinition.id, { currentStatus: IngestionTaskStatus.FAILED, lastRun: new Date(), lastRunStatus: finalStatus });
        }

        const sourceIdentifier = this.getSourceIdentifier(taskDefinition.source.pluginType, taskDefinition.source.config);
        if (sourceIdentifier && finalStatus.data) {
            const updates: Partial<WebhookRegistryEntry> = {};
            if (finalStatus.data.startPageToken !== undefined) {
                updates.startPageToken = finalStatus.data.startPageToken;
            }
            if (finalStatus.data.nextPageToken !== undefined) {
                updates.nextPageToken = finalStatus.data.nextPageToken;
            }
            if (finalStatus.data.otherCrawlerSpecificTokens !== undefined) {
                updates.otherCrawlerSpecificTokens = finalStatus.data.otherCrawlerSpecificTokens;
            }

            if (Object.keys(updates).length > 0) {
                try {
                    let webhookEntry = await this.dbService.getWebhookRegistration(sourceIdentifier);
                    if (!webhookEntry) {
                        webhookEntry = {
                            sourceIdentifier: sourceIdentifier,
                            endpointId: (taskDefinition.trigger as WebhookTrigger).endpointId || 'unknown',
                            secret: (taskDefinition.trigger as WebhookTrigger).secret || 'unknown',
                            externalWebhookId: (taskDefinition.trigger as WebhookTrigger).externalWebhookId || 'unknown',
                            registeredTasks: [taskDefinition.id],
                            webhookFlag: true
                        };
                        await this.dbService.saveWebhookRegistration(webhookEntry);
                        logger.warn(`GlobalIngestionLifecycleManager: Created missing webhook registry entry for '${sourceIdentifier}' to save tokens.`);
                    }
                    await this.dbService.updateWebhookRegistration(sourceIdentifier, updates);
                    logger.info(`GlobalIngestionLifecycleManager: Updated tokens for webhook registration '${sourceIdentifier}'.`);
                } catch (dbError: any) {
                    logger.error(`GlobalIngestionLifecycleManager: Failed to update webhook registration tokens for '${sourceIdentifier}': ${dbError.message}`, { dbError });
                }
            }
        }
        return finalStatus;
    }
    
    // onTaskCompleted and onTaskFailed now just log, as status updates are handled in runOrchestrator
    private onTaskCompleted(taskId: string, status: GSStatus) {
        logger.info(`Task '${taskId}' completed successfully.`);
    }
    
    private onTaskFailed(taskId: string, status: GSStatus) {
        logger.error(`Task '${taskId}' failed.`);
    }

    private getSourceIdentifier(pluginType: string, sourceConfig: any): string | undefined {
        switch (pluginType) {
            case 'git-crawler':
                return sourceConfig.repoUrl;
            case 'googledrive-crawler':
                return sourceConfig.folderId;
            case 'teams-chat-crawler':
                return sourceConfig.meetingId || sourceConfig.chatId; 
            case 'http-crawler':
                return sourceConfig.url;
            default:
                logger.warn(`Unsupported plugin type '${pluginType}' for source identification.`);
                return undefined;
        }
    }
     private extractRepoNameFromUrl(repoUrl: string): string {
        try {
            const url = new URL(repoUrl);
            const pathParts = url.pathname.split('/').filter(part => part); // Remove empty strings
            if (url.hostname === 'github.com' && pathParts.length >= 2) {
                return `${pathParts[0]}/${pathParts[1]}`;
            }
        } catch (e: any) {
            logger.warn(`Failed to parse repo URL '${repoUrl}': ${e.message}`);
        }
        return repoUrl; // Fallback to original if not a GitHub URL or parsing fails
    }

    public async registerWebhook(taskDefinition: IngestionTaskDefinition): Promise<GSStatus> {
        const trigger = taskDefinition.trigger as WebhookTrigger;
        const sourceConfig = taskDefinition.source.config;
        const pluginType = taskDefinition.source.pluginType;
        const taskId = taskDefinition.id;
        //logger.debug("Entered into registerwebhook")
        const sourceIdentifier = this.getSourceIdentifier(pluginType, sourceConfig);
        if (!sourceIdentifier) {
            return new GSStatus(false, 400, `Webhook registration not supported or source identifier missing for plugin type '${pluginType}'.`);
        }
        //logger.debug(`---------sourceIdentifier:${sourceIdentifier}`)
        try {
            let existingWebhookEntry = await this.dbService.getWebhookRegistration(sourceIdentifier);
            let externalWebhookId: string;
            let secret: string;
            let registrationResultData: any = {};
           // logger.debug(`----------------existingWebhookEntry:${existingWebhookEntry}`)
            if (existingWebhookEntry) {
                if (!existingWebhookEntry.registeredTasks.includes(taskId)) {
                    existingWebhookEntry.registeredTasks.push(taskId);
                    await this.dbService.updateWebhookRegistration(sourceIdentifier, { registeredTasks: existingWebhookEntry.registeredTasks, webhookFlag: true });
                }
                externalWebhookId = existingWebhookEntry.externalWebhookId;
                secret = existingWebhookEntry.secret;
                trigger.externalWebhookId = externalWebhookId;
                trigger.secret = secret;
                await this.dbService.updateTask(taskId, { trigger: trigger });
                logger.info(`Task '${taskId}' associated with existing webhook for source '${sourceIdentifier}'.`);
                return new GSStatus(true, 200, `Task associated with existing webhook for '${sourceIdentifier}'.`);
            } else {
                secret = crypto.randomBytes(20).toString('hex');
                let registrationStatus:any;
              //  logger.debug("-------------proceeding to webhook registration-------------") 
                if (pluginType === 'git-crawler') {
                    if (!trigger.credentials) {
                        throw new Error("Git crawler webhook registration requires credentials in trigger.");
                    }
                    const repoName = this.extractRepoNameFromUrl(sourceConfig.repoUrl);
                    registrationStatus = await DataSourceApiUtils.registerWebhook(
                        pluginType,
                        repoName,
                        trigger.callbackurl,
                        secret,
                        trigger.credentials
                    );
                } else if (pluginType === 'googledrive-crawler') {
                    if (!trigger.credentials) {
                        throw new Error("Google Drive crawler webhook registration requires credentials in trigger.");
                    }
                    registrationStatus = await DataSourceApiUtils.registerWebhook(
                        pluginType,
                        sourceConfig.folderId,
                        trigger.callbackurl,
                        secret,
                        trigger.credentials,
                    );
                } else {
                    return new GSStatus(false, 400, "Unsupported webhook plugin type for external registration.");
                }
                // console.log("DEBUG START --------registrationStatus from webhook reg---------------")
                // console.log("registrationStatus:",registrationStatus)
                 
                if (!registrationStatus.success || !registrationStatus.externalId) {
                    throw new Error(`External webhook registration failed: ${registrationStatus.error || 'Unknown error'}`);
                }
                // console.log("DEBUG END------------------------------------")
                externalWebhookId = registrationStatus.externalId;
                // FIX: Assign tokens directly from registrationStatus
                logger.debug(`---------- externalWebhookId:${ externalWebhookId}-----------------`)
                registrationResultData = {
                    startpageToken: registrationStatus.startpageToken,
                    nextPageToken: registrationStatus.nextPageToken,
                    otherCrawlerSpecificTokens: registrationStatus.otherCrawlerSpecificTokens
                };
                //  logger.debug(`--------------registrationResultData:${registrationResultData}`)
                logger.info(`New webhook registered for '${sourceIdentifier}' with external ID '${externalWebhookId}'.`);

                const newWebhookEntry: WebhookRegistryEntry = {
                    sourceIdentifier: sourceIdentifier,
                    endpointId: trigger.endpointId,
                    secret: secret,
                    externalWebhookId: externalWebhookId,
                    registeredTasks: [taskId],
                    webhookFlag: true,
                    startPageToken: registrationResultData.startpageToken || undefined,
                    nextPageToken: registrationResultData.nextPageToken || undefined,
                    otherCrawlerSpecificTokens: registrationResultData.otherCrawlerSpecificTokens || undefined
                };
                //logger.debug("-------------saving webhookEntry to db-----------")
                await this.dbService.saveWebhookRegistration(newWebhookEntry);
                
                trigger.externalWebhookId = externalWebhookId;
                trigger.secret = secret;
                taskDefinition.startPageToken = newWebhookEntry.startPageToken;
                taskDefinition.nextPageToken = newWebhookEntry.nextPageToken;
                taskDefinition.otherCrawlerSpecificTokens = newWebhookEntry.otherCrawlerSpecificTokens;
                //logger.debug("-----------updating task-----------------") 
                await this.dbService.updateTask(taskId, { trigger: trigger, ...newWebhookEntry });
                
                return new GSStatus(true, 200, `Webhook registered successfully for '${sourceIdentifier}'.`);
            }
        } catch (error: any) {
            logger.error(`Failed to register webhook for task ${taskId} and source '${sourceIdentifier}': ${error.message}`, { error });
            return new GSStatus(false, 500, `Failed to register webhook: ${error.message}`);
        }
    }

    public async deregisterWebhook(taskId: string): Promise<GSStatus> {
        const task = await this.dbService.getTask(taskId);
        if (!task || task.trigger.type !== 'webhook') {
            return new GSStatus(false, 404, `Task with ID ${taskId} not found or is not a webhook task.`);
        }

        const trigger = task.trigger as WebhookTrigger;
        const sourceConfig = task.source.config;
        const pluginType = task.source.pluginType;
        
        const sourceIdentifier = this.getSourceIdentifier(pluginType, sourceConfig);
        if (!sourceIdentifier) {
            return new GSStatus(false, 400, `Webhook deregistration not supported or source identifier missing for plugin type '${pluginType}'.`);
        }

        try {
            const webhookEntry = await this.dbService.getWebhookRegistration(sourceIdentifier);
            if (!webhookEntry) {
                logger.warn(`No webhook entry found for key '${sourceIdentifier}'. Assuming it's already deregistered.`);
                return new GSStatus(true, 200, "Webhook already deregistered.");
            }

            const updatedRegisteredTasks = webhookEntry.registeredTasks.filter(id => id !== taskId);
            await this.dbService.updateWebhookRegistration(sourceIdentifier, { registeredTasks: updatedRegisteredTasks });
            logger.info(`Task '${taskId}' removed from webhook registry for '${sourceIdentifier}'. Remaining tasks: ${updatedRegisteredTasks.length}.`);

            if (updatedRegisteredTasks.length === 0) {
                logger.info(`Last task for webhook '${sourceIdentifier}' was removed. Deregistering webhook externally.`);
                let deregistrationStatus: { success: boolean; message?: string };
                
                if (!webhookEntry.externalWebhookId) {
                    logger.warn(`Cannot deregister external webhook for '${sourceIdentifier}': externalWebhookId is missing from registry entry.`);
                    return new GSStatus(false, 500, "Cannot deregister webhook: external ID missing.");
                }

                if (pluginType === 'git-crawler') {
                    if (!trigger.credentials) {
                        throw new Error("Git crawler webhook deregistration requires credentials in trigger.");
                    }
                    const repoName = this.extractRepoNameFromUrl(sourceConfig.repoUrl);
                    deregistrationStatus = await DataSourceApiUtils.deregisterWebhook(
                        pluginType,
                        repoName, // resourceId for Git is repoUrl
                        webhookEntry.externalWebhookId,
                        trigger.credentials,
                    );
                } else if (pluginType === 'googledrive-crawler') {
                    if (!trigger.credentials) {
                        throw new Error("Google Drive crawler webhook deregistration requires credentials in trigger.");
                    }
                    // FIX: Added 'sourceIdentifier' (which is folderId for GDrive) as the resourceId
                    deregistrationStatus = await DataSourceApiUtils.deregisterWebhook(
                        pluginType,
                        webhookEntry.secret, 
                        webhookEntry.externalWebhookId,
                        trigger.credentials,
                    );
                } else {
                    return new GSStatus(false, 400, "Unsupported webhook plugin type for external deregistration.");
                }

                if (!deregistrationStatus.success) {
                    throw new Error(deregistrationStatus.message);
                }
                
                await this.dbService.deleteWebhookRegistration(sourceIdentifier);
                logger.info(`External webhook '${webhookEntry.externalWebhookId}' for '${sourceIdentifier}' deregistered.`);
                return new GSStatus(true, 200, "Webhook deregistered successfully.");
            }
            
            return new GSStatus(true, 200, "Task removed, but other tasks are still using this webhook.");
        } catch (error: any) {
            logger.error(`Failed to deregister webhook for task ${taskId} and source '${sourceIdentifier}': ${error.message}`, { error });
            return new GSStatus(false, 500, `Failed to deregister webhook: ${error.message}`);
        }
    }
}
