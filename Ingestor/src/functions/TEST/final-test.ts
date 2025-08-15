// src/functions/TEST/final-test.ts

import { GlobalIngestionLifecycleManager } from '../Scheduler/GlobalIngestionLifecycleManager'
import { FileSystemDestinationAdapter } from '../Scheduler/FileSystemDestinationAdapter' 
import { IngestionTaskStatus, IngestionTaskDefinition, IngestionData, IngestionEvents, GSDataSource, IngestionDataTransformer } from '../Scheduler/interfaces' 

import { logger } from '@godspeedsystems/core';
const appConfig = require('config'); 

const globalIngestionManager = GlobalIngestionLifecycleManager.getInstance();

export async function setupGlobalIngestionManager() {
    logger.info("--- Setting up GlobalIngestionLifecycleManager for Integration Test ---");

    await globalIngestionManager.sources(['git-crawler', 'googledrive-crawler', 'http-crawler']);
    logger.info("Registered default data sources via .sources() method.");

    globalIngestionManager.registerDestination('file-system-destination', FileSystemDestinationAdapter);
    logger.info("Registered 'file-system-destination' destination.");

    // --- Custom Source/Transformer Registration (Hybrid Mode) ---
    // This section demonstrates how users can register their own custom DataSource implementations
    // or override default transformers for existing sources. This provides full flexibility beyond
    // the array-based default registration.
    /*
    // Example: Registering a custom 'my-custom-crawler' with its own transformer
    class MyCustomCrawlerDataSource extends GSDataSource {
        constructor(configWrapper: any) { super(configWrapper); }
        async initClient(): Promise<any> { logger.info("Custom client init."); return {}; }
        async execute(ctx: any, payload: any): Promise<any> {
            logger.info("Running custom crawler with payload:", payload);
            const rawData = [{ id: 'custom-item-1', value: 'Hello Custom Data!' }];
            return new GSStatus(true, 200, "Custom crawl done.", { data: rawData });
        }
    }
    // Define a custom transformer function adhering to the IngestionDataTransformer signature
    const myCustomTransformer: IngestionDataTransformer = async (rawData: any[], initialPayload?: any): Promise<IngestionData[]> => {
        logger.info("Running custom transformer on raw data:", rawData);
        return rawData.map(item => ({ 
            id: `transformed-${item.id}`, 
            content: `Transformed: ${JSON.stringify(item.value)}`,
            metadata: { originalPayload: initialPayload, processedBy: 'my-custom-transformer' }
        }));
    };
    // Register the custom source with its custom transformer
    globalIngestionManager.registerSource('my-custom-crawler', MyCustomCrawlerDataSource, myCustomTransformer);
    logger.info("Registered 'my-custom-crawler' source with a custom transformer.");
    */

    // --- Load and Schedule Tasks from config/default.yaml ---
    const tasksConfig: { [key: string]: IngestionTaskDefinition } = appConfig.get('tasks'); 
    
    if (tasksConfig) {
        for (const taskId in tasksConfig) {
            if (tasksConfig.hasOwnProperty(taskId)) {
                const taskDefinition = JSON.parse(JSON.stringify(tasksConfig[taskId])); 
                taskDefinition.id = taskId; 
                
                // The taskDefinition will be scheduled exactly as defined in default.yaml.
                // Sensitive values like credentials and callback URLs are expected to be
                // hardcoded in the YAML for this specific setup, as per the current instruction.
                // In a production setup, these would typically be injected from process.env here.

                await globalIngestionManager.scheduleTask(taskDefinition);
                logger.info(`Scheduled task from config: '${taskDefinition.id}' (type: ${taskDefinition.trigger.type}).`);
            }
        }
    } else {
        logger.warn("No 'tasks' found in configuration. No tasks scheduled from config files.");
        // --- Fallback for Task Definition (if YAML loading fails or is not used) ---
        // If loading tasks from config/default.yaml fails or if you prefer to define tasks
        // directly in code, you can uncomment and define them here.
        // This provides a robust fallback but is less flexible for large numbers of tasks.
        logger.info("Attempting to schedule hardcoded fallback tasks.");
        const hardcodedGitWebhookTask: IngestionTaskDefinition = {
            id: 'hardcoded-webhook-git-clone',
            name: 'Hardcoded Git Webhook Task',
            enabled: true,
            source: {
                pluginType: 'git-crawler',
                config: {
                    repoUrl: 'https://github.com/example-org/hardcoded-repo',
                    branch: 'dev',
                    depth: 1,
                }
            },
            destination: {
                pluginType: 'file-system-destination',
                config: { outputPath: './crawled_output/hardcoded-webhook-git' }
            },
            trigger: {
                type: 'webhook',
                credentials: "your_github_pat_here", 
                endpointId:'/webhook/github/',
                callbackurl: "https://your-ngrok-url.example.com/api/v1/webhook/github/" 
            },
            currentStatus: IngestionTaskStatus.SCHEDULED,
        };
        await globalIngestionManager.scheduleTask(hardcodedGitWebhookTask);
        logger.info(`Scheduled hardcoded task: '${hardcodedGitWebhookTask.id}'.`);

        const hardcodedCronGoogleDriveTask: IngestionTaskDefinition = {
            id: 'hardcoded-cron-gdrive',
            name: 'Hardcoded GDrive Cron Task',
            enabled: true,
            source: {
                pluginType: 'googledrive-crawler',
                config: {
                    serviceAccountKeyPath: "./path/to/example-service-account-key.json", 
                    userToImpersonateEmail: "your_service_account_email@example.com", 
                    folderId: "example_google_drive_folder_id", 
                }
            },
            destination: {
                pluginType: 'file-system-destination',
                config: { outputPath: './crawled_output/hardcoded-cron-gdrive' }
            },
            trigger: {
                type: 'cron',
                expression: '*/2 * * * *' 
            },
            currentStatus: IngestionTaskStatus.SCHEDULED,
        };
        await globalIngestionManager.scheduleTask(hardcodedCronGoogleDriveTask);
        logger.info(`Scheduled hardcoded task: '${hardcodedCronGoogleDriveTask.id}'.`);
    }

    const currentTasksInManager = await globalIngestionManager.listTasks();
    logger.info(`[DEBUG test-run] Current tasks in manager's DB: ${JSON.stringify(currentTasksInManager, null, 2)}`);

// --- Event Listeners for Debugging ---
    // These listeners subscribe to events emitted by the GlobalIngestionLifecycleManager's event bus.
    // They are crucial for observing the data flow and task lifecycle in real-time,
    // and for debugging the ingestion pipeline's behavior.
    let capturedIngestionData: IngestionData[] = []; // Variable to capture transformed data for post-execution analysis

    // Listener for when an ingestion task completes successfully.
    // It logs the task ID and the success message from the returned status.
    globalIngestionManager.getEventBus().on(IngestionEvents.TASK_COMPLETED, (taskId: string, status: any) => { 
        logger.info(`[Event Listener] Task '${taskId}' completed with status: ${status.message || status}`);
    });

    // Listener for when an ingestion task fails.
    // It logs the task ID and the error details, which is vital for troubleshooting.
    globalIngestionManager.getEventBus().on(IngestionEvents.TASK_FAILED, (taskId: string, error: any) => {
        logger.error(`[Event Listener] Task '${taskId}' FAILED:`, error);
    });

    // Listener for when data has been transformed by a transformer.
    // This event provides access to the standardized IngestionData items.
    globalIngestionManager.getEventBus().on(IngestionEvents.DATA_TRANSFORMED, (transformedData: IngestionData[], taskId: string) => {
        logger.info(`[Event Listener] Task '${taskId}' emitted DATA_TRANSFORMED. Transformed ${transformedData.length} items.`);
        
        // Example: Capture transformed data into a variable.
        // This allows you to inspect the processed data outside the event listener's scope,
        // which is useful for integration tests or advanced debugging scenarios.
        capturedIngestionData = capturedIngestionData.concat(transformedData);
        logger.debug(`[Event Listener] Captured ${transformedData.length} items. Total captured: ${capturedIngestionData.length}.`);

        // Log a detailed sample of the first few transformed data items.
        // JSON.stringify is used to display the full structure, including metadata.
        transformedData.slice(0, 2).forEach((item, index) => {
            logger.debug(`     Sample Item ${index + 1}: ${JSON.stringify(item, null, 2)}`);
        });
        // If there are more than 2 items, indicate that to avoid excessive logging.
        if (transformedData.length > 2) {
            logger.debug(`     ...and ${transformedData.length - 2} more items.`);
        }
    });

    // 4. Initialize and Start the Manager
    await globalIngestionManager.init();
    await globalIngestionManager.start();
    
    logger.info("--- GlobalIngestionLifecycleManager setup complete and ready for triggers. ---");
} 
setupGlobalIngestionManager(); 
export { globalIngestionManager };

