// src\datasources\types\gdrive-crawler.ts

import { GSContext, GSDataSource, GSStatus, logger, PlainObject } from "@godspeedsystems/core";
import { google, drive_v3 } from 'googleapis';
import { JWT } from 'google-auth-library';
import { Readable } from 'stream';
import { IngestionData } from '../../functions/Scheduler/interfaces';
import { GaxiosResponse } from 'gaxios';
import { IngestionTaskDefinition } from "../../functions/Scheduler/interfaces";
import * as fs from 'fs';

interface ServiceAccountKey {
    type?: string;
    project_id?: string;
    private_key_id?: string;
    private_key?: string;
    client_email?: string;
    client_id?: string;
    auth_uri?: string;
    token_uri?: string;
    auth_provider_x509_cert_url?: string;
    client_x509_cert_url?: string;
    universe_domain?: string;
}

export interface GoogleDriveCrawlerConfig {
    folderId?: string;
    authType?: 'service_account';
    serviceAccountKeyPath?: string;
    serviceAccountKey?: string;
    pageSize?: number;
}

export interface GoogleDriveCrawlerPayload extends PlainObject {
    taskDefinition: IngestionTaskDefinition;
    webhookPayload?: { // Optional, present only for webhook triggers
        channelResourceId?: string;
        resourceState?: 'exists' | 'not_exists' | 'sync' | 'add' | 'update' | 'trash';
    };
    startPageToken?: string;
    nextPageToken?: string;
    externalResourceId?: string;
    changeType?: 'UPSERT' | 'DELETE' | 'UNKNOWN';
}

export default class DataSource extends GSDataSource {
    private driveClient: drive_v3.Drive | undefined;
    private jwtClient: JWT | undefined;
    public config: GoogleDriveCrawlerConfig;

    constructor(configWrapper: { config: GoogleDriveCrawlerConfig } | GoogleDriveCrawlerConfig) {
        super(configWrapper);

        const initialConfig = ('config' in configWrapper) ? (configWrapper as { config: GoogleDriveCrawlerConfig }).config : (configWrapper as GoogleDriveCrawlerConfig);

        this.config = {
            authType: 'service_account',
            pageSize: 100,
            ...initialConfig,
        } as GoogleDriveCrawlerConfig;

        // FIX: Removed folderId validation from constructor.
        // It will now be validated in the execute method when the crawler is actually run.
        logger.info(`GoogleDriveCrawler instance created for folder: ${this.config.folderId || 'Not specified (lazy init expected)'}`);
    }

    /**
     * Initializes an authenticated Google Drive API client using a service account key.
     * This method is idempotent and will not re-initialize if a client already exists.
     */
    public async initClient(): Promise<GSStatus> {
        if (this.driveClient) {
            return new GSStatus(true, 200, "Google Drive client already initialized.");
        }

        // FIX: Moved folderId validation here from constructor.
        // This ensures validation occurs only when the client is being initialized for execution.
        if (!this.config.folderId || this.config.folderId === "your_google_drive_folder_id") {
            logger.warn("GoogleDriveCrawler: 'folderId' is required and valid for client initialization. Client not initialized.");
            return new GSStatus(false, 400, "Missing or invalid 'folderId' in configuration. Client not initialized.");
        }

        logger.info("GoogleDriveCrawler: Initializing Google Drive client...");
        let serviceAccountKey: ServiceAccountKey;

        // Load the service account key from either a direct string or a file path.
        try {
            if (this.config.serviceAccountKey) {
                serviceAccountKey = JSON.parse(this.config.serviceAccountKey);
                logger.info("GoogleDriveCrawler: Using service account key from 'serviceAccountKey' config.");
            } else if (this.config.serviceAccountKeyPath) {
                const keyFileContent = fs.readFileSync(this.config.serviceAccountKeyPath, 'utf8');
                serviceAccountKey = JSON.parse(keyFileContent);
                logger.info(`GoogleDriveCrawler: Using service account key from file: ${this.config.serviceAccountKeyPath}.`);
            } else {
                logger.warn("GoogleDriveCrawler: No 'serviceAccountKey' or 'serviceAccountKeyPath' provided. Cannot initialize Google Drive client.");
                return new GSStatus(false, 400, "Missing service account key configuration. Client not initialized.");
            }
        } catch (error: any) {
            logger.warn(`GoogleDriveCrawler: Failed to load/parse service account key: ${error.message}. Client will not be initialized.`, { error });
            return new GSStatus(false, 500, `Invalid service account key: ${error.message}`);
        }

        if (!serviceAccountKey.client_email || !serviceAccountKey.private_key) {
            logger.warn("GoogleDriveCrawler: Service account key is missing 'client_email' or 'private_key'. Client will not be initialized.");
            return new GSStatus(false, 400, "Incomplete service account key. Client not initialized.");
        }

        // Create and authorize a JWT client for server-to-server authentication.
        this.jwtClient = new google.auth.JWT({
            email: serviceAccountKey.client_email,
            key: serviceAccountKey.private_key,
            scopes: ['https://www.googleapis.com/auth/drive.readonly'],
        });

        try {
            await this.jwtClient.authorize();
            logger.info("GoogleDriveCrawler: JWT client authorized successfully.");
            this.driveClient = google.drive({ version: 'v3', auth: this.jwtClient });
            logger.info("GoogleDriveCrawler client initialized and ready.");
            return new GSStatus(true, 200, "Client initialized.");
        } catch (error: any) {
            logger.warn(`GoogleDriveCrawler: Failed to authorize JWT client or create Drive client: ${error.message}. Client will not be initialized.`, { error });
            return new GSStatus(false, 500, `Authentication failed: ${error.message}`);
        }
    }

    async execute(ctx: GSContext, args: PlainObject): Promise<GSStatus> {
        const initialPayload = args as GoogleDriveCrawlerPayload;

        const initStatus = await this.initClient();
        if (!initStatus.success) {
            logger.warn(`GoogleDriveCrawler: Execution skipped because client could not be initialized.`);
            return initStatus;
        }

        const fetchedAt = new Date();
        const ingestionData: IngestionData[] = [];

        const isWebhookTriggered = !!initialPayload?.webhookPayload;
        logger.info(`GoogleDriveCrawler: Operating in ${isWebhookTriggered ? 'webhook' : 'standard (full scan)'} mode.`);

        // This block handles incremental "delta sync" crawls triggered by a webhook.
        if (isWebhookTriggered) {
            let changesPageToken: string | undefined;

            // Determine the starting point for fetching changes from the Drive API.
            if (initialPayload?.startPageToken) {
                changesPageToken = initialPayload.startPageToken;
                logger.info(`GoogleDriveCrawler: Performing a delta sync from startPageToken provided by orchestrator: ${changesPageToken}`);
            } else if (initialPayload?.nextPageToken) {
                changesPageToken = initialPayload.nextPageToken;
                logger.info(`GoogleDriveCrawler: Performing a delta sync from nextPageToken provided by orchestrator: ${changesPageToken}`);
            } else {
                // If no token exists, this is the first webhook notification.
                // We must get the current page token and store it for the *next* sync.
                logger.info("GoogleDriveCrawler: No prior page token found. Getting initial page token for change tracking (first webhook trigger or fresh start).");
                try {
                    const startTokenRes = await this.driveClient!.changes.getStartPageToken({ fields: 'startPageToken' });
                    changesPageToken = startTokenRes.data.startPageToken!;
                    logger.info(`GoogleDriveCrawler: Initial startPageToken acquired: ${changesPageToken}`);
                    
                    return new GSStatus(true, 200, "Initial webhook received. Set up for future change tracking.", {
                        crawledCount: 0,
                        data: [],
                        startPageToken: changesPageToken,
                    });
                } catch (getTokenError: any) {
                    logger.error(`GoogleDriveCrawler: Failed to get initial startPageToken: ${getTokenError.message}`);
                    return new GSStatus(false, 500, `Failed to get initial page token: ${getTokenError.message}`);
                }
            }

            let newStartPageToken = changesPageToken;
            let moreChanges = true;

            // Paginate through all changes since the last known token.
            while (moreChanges) {
                try {
                    const changesRes: GaxiosResponse<drive_v3.Schema$ChangeList> = (await this.driveClient!.changes.list({
                        pageToken: changesPageToken,
                        fields: 'nextPageToken, newStartPageToken, changes(fileId, removed, file(id, name, mimeType, webViewLink, webContentLink, size, createdTime, modifiedTime, parents, trashed))',
                        pageSize: this.config.pageSize,
                        spaces: 'drive',
                    })) as unknown as GaxiosResponse<drive_v3.Schema$ChangeList>;

                    newStartPageToken = changesRes.data.newStartPageToken || newStartPageToken;
                    const changes = changesRes.data.changes || [];
                    logger.info(`GoogleDriveCrawler: Found ${changes.length} changes since page token.`);

                    for (const change of changes) {
                        const fileId = change.fileId;
                        const file = change.file;

                        if (!fileId) continue;

                        if (change.removed) {
                            ingestionData.push({
                                id: fileId,
                                content: '',
                                url: `https://drive.google.com/file/d/${fileId}/view`,
                                statusCode: 200,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    fileId: fileId,
                                    changeType: 'DELETE',
                                    ingestionType: 'gdrive_delta_sync_removal',
                                }
                            });
                            logger.info(`GoogleDriveCrawler: Processed permanent removal event for file ID '${fileId}'.`);
                        } else if (file) {
                            const fileParents = file.parents ?? [];
                            
                            if (file.trashed) {
                                ingestionData.push({
                                    id: fileId,
                                    content: '',
                                    url: file.webViewLink || file.webContentLink || `https://drive.google.com/file/d/${fileId}/view`,
                                    statusCode: 200,
                                    fetchedAt: fetchedAt,
                                    metadata: {
                                        fileId: fileId,
                                        fileName: file.name ?? 'unknown-name',
                                        mimeType: file.mimeType ?? 'application/octet-stream',
                                        fileSize: file.size ? parseInt(file.size, 10) : 0,
                                        changeType: 'DELETE',
                                        ingestionType: 'gdrive_delta_sync_trash',
                                        createdTime: file.createdTime ?? '',
                                        modifiedTime: file.modifiedTime ?? '',
                                        parents: fileParents,
                                        trashed: true,
                                    }
                                });
                                logger.info(`GoogleDriveCrawler: Processed trash event for file '${file.name}' (ID: ${fileId}).`);
                            } else if (this.config.folderId && !fileParents.includes(this.config.folderId)) {
                                // Skip files that are not in the specified folder.
                                continue;
                            } else {
                                try {
                                    const fileContent = await this._getFileContent(this.driveClient!, fileId, file.mimeType ?? 'application/octet-stream');
                                    ingestionData.push({
                                        id: fileId,
                                        content: fileContent || '',
                                        url: file.webViewLink || file.webContentLink || `https://drive.google.com/file/d/${fileId}/view`,
                                        statusCode: 200,
                                        fetchedAt: fetchedAt,
                                        metadata: {
                                            fileId: fileId,
                                            fileName: file.name ?? 'unknown-name',
                                            mimeType: file.mimeType ?? 'application/octet-stream',
                                            fileSize: file.size ? parseInt(file.size, 10) : 0,
                                            changeType: 'UPSERT',
                                            ingestionType: 'gdrive_delta_sync',
                                            createdTime: file.createdTime ?? '',
                                            modifiedTime: file.modifiedTime ?? '',
                                            parents: fileParents,
                                        }
                                    });
                                    logger.info(`GoogleDriveCrawler: Ingested content for file '${file.name}' (ID: ${fileId}) via delta sync.`);
                                } catch (contentFetchError: any) {
                                    logger.error(`GoogleDriveCrawler: Failed to fetch content for file '${fileId}': ${contentFetchError.message}`);
                                    ingestionData.push({
                                        id: fileId,
                                        content: `Error fetching file: ${contentFetchError.message}`,
                                        url: file.webViewLink || file.webContentLink || `https://drive.google.com/file/d/${fileId}/view`,
                                        statusCode: 500,
                                        fetchedAt: fetchedAt,
                                        metadata: {
                                            fileId: fileId,
                                            fileName: file.name ?? 'unknown-name',
                                            mimeType: file.mimeType ?? 'application/octet-stream',
                                            ingestionType: 'gdrive_full_scan_fetch_failed',
                                            error: contentFetchError.message
                                        }
                                    });
                                }
                            }
                        }
                    }

                    changesPageToken = changesRes.data.nextPageToken ?? '';
                    if (!changesPageToken) {
                        moreChanges = false;
                    }

                } catch (error: any) {
                    logger.error(`GoogleDriveCrawler: Failed to get changes list: ${error.message}`);
                    return new GSStatus(false, 500, `Delta sync failed: ${error.message}`);
                }
            }
            return new GSStatus(true, 200, "Google Drive webhook delta sync successful.", {
                crawledCount: ingestionData.length,
                data: ingestionData,
                startPageToken: newStartPageToken,
            });
        } 
        // This block handles a full scan of all files in the configured folder.
        else {
            // FIX: FolderId validation is now performed by initClient().
            // If initClient() succeeded, folderId should be valid.
            // This check here is a safety net for unexpected scenarios.
            if (!this.config.folderId) {
                logger.error("GoogleDriveCrawler: Fatal error, 'folderId' became invalid during standard crawling mode execution.");
                return new GSStatus(false, 500, "Internal error: 'folderId' missing during standard crawling mode execution.");
            }

            logger.info(`GoogleDriveCrawler: Operating in standard (full scan) mode for folder: ${this.config.folderId}.`);
            let currentScanPageToken: string | undefined = initialPayload?.nextPageToken;
            let totalFilesListed = 0;

            if (currentScanPageToken) {
                logger.info(`GoogleDriveCrawler: Resuming full scan from pageToken: ${currentScanPageToken}`);
            }

            try {
                let nextPageToken: string | null | undefined;
                // Use a do-while loop to handle paginated results from the Drive API.
                do {
                    const listResult = await this._listFiles(this.driveClient!, this.config.folderId, currentScanPageToken, this.config.pageSize);
                    totalFilesListed += listResult.files?.length || 0;
                    nextPageToken = listResult.nextPageToken;
                    if (listResult.files) {
                        for (const file of listResult.files) {
                            const fileId = file.id;
                            if (fileId && file.name) {
                                if (file.trashed) {
                                    ingestionData.push({
                                        id: fileId,
                                        content: '',
                                        url: file.webViewLink || file.webContentLink || `https://drive.google.com/file/d/${fileId}/view`,
                                        statusCode: 200,
                                        fetchedAt: fetchedAt,
                                        metadata: {
                                            fileId: fileId,
                                            fileName: file.name ?? 'unknown-name',
                                            mimeType: file.mimeType ?? 'application/octet-stream',
                                            fileSize: file.size ? parseInt(file.size, 10) : 0,
                                            changeType: 'DELETE',
                                            ingestionType: 'gdrive_full_scan_trash',
                                            createdTime: file.createdTime ?? '',
                                            modifiedTime: file.modifiedTime ?? '',
                                            parents: file.parents ?? [],
                                            trashed: true,
                                        }
                                    });
                                    logger.info(`GoogleDriveCrawler: Processed trashed file '${file.name}' (ID: ${fileId}) during full scan.`);
                                } else {
                                    try {
                                        const fileContent = await this._getFileContent(this.driveClient!, fileId, file.mimeType ?? 'application/octet-stream');
                                        ingestionData.push({
                                            id: fileId,
                                            content: fileContent || '',
                                            url: file.webViewLink || file.webContentLink || `https://drive.google.com/file/d/${fileId}/view`,
                                            statusCode: 200,
                                            fetchedAt: fetchedAt,
                                            metadata: {
                                                fileId: fileId,
                                                fileName: file.name ?? 'unknown-name',
                                                mimeType: file.mimeType ?? 'application/octet-stream',
                                                fileSize: file.size ? parseInt(file.size, 10) : 0,
                                                createdTime: file.createdTime ?? '',
                                                modifiedTime: file.modifiedTime ?? '',
                                                parents: file.parents ?? [],
                                                ingestionType: 'gdrive_full_scan'
                                            }
                                        });
                                    } catch (fetchError: any) {
                                        logger.warn(`GoogleDriveCrawler: Failed to fetch content for file '${file.name}' (ID: ${fileId}): ${fetchError.message}`);
                                        ingestionData.push({
                                            id: fileId,
                                            content: `Error fetching file: ${fetchError.message}`,
                                            url: file.webViewLink || file.webContentLink || `https://drive.google.com/file/d/${fileId}/view`,
                                            statusCode: 500,
                                            fetchedAt: fetchedAt,
                                            metadata: {
                                                fileId: fileId,
                                                fileName: file.name ?? 'unknown-name',
                                                mimeType: file.mimeType ?? 'application/octet-stream',
                                                ingestionType: 'gdrive_full_scan_fetch_failed',
                                                error: fetchError.message
                                            }
                                        });
                                    }
                                }
                            }
                        }
                    }
                    
                    // This line prevents an infinite loop by updating the page token for the next iteration.
                    currentScanPageToken = nextPageToken || undefined;

                } while (currentScanPageToken);

                logger.info(`GoogleDriveCrawler: Completed full scan. Listed ${totalFilesListed} files and ingested ${ingestionData.length} items.`);
                return new GSStatus(true, 200, "Google Drive folder scan successful.", {
                    crawledCount: ingestionData.length,
                    data: ingestionData,
                    nextPageToken: nextPageToken
                });

            } catch (error: any) {
                logger.error(`GoogleDriveCrawler: Failed during standard Drive scan: ${error.message}`, { error });
                return new GSStatus(false, 500, `Google Drive folder scan failed: ${error.message}`);
            }
        }
    }

    /**
     * A helper method to list files within a specific Google Drive folder.
     */
    private async _listFiles(driveClient: drive_v3.Drive, folderId: string, pageToken?: string, pageSize?: number): Promise<drive_v3.Schema$FileList> {
        const query = `'${folderId}' in parents and trashed = false`;
        const fields = 'nextPageToken, files(id, name, mimeType, webViewLink, webContentLink, size, createdTime, modifiedTime, parents, trashed)';

        const res = await driveClient.files.list({
            q: query,
            fields: fields,
            spaces: 'drive',
            pageToken: pageToken,
            pageSize: pageSize,
        });
        return res.data;
    }

    /**
     * Downloads the content of a file, with special logic for native Google Docs.
     */
    private async _getFileContent(driveClient: drive_v3.Drive, fileId: string, mimeType: string): Promise<string | Buffer | undefined> {
        let isGoogleDoc = false;
        let exportMimeType: string | undefined;

        // Determine if the file is a native Google Doc and set the appropriate export format.
        if (mimeType === 'application/vnd.google-apps.document') {
            isGoogleDoc = true;
            exportMimeType = 'text/plain';
        } else if (mimeType === 'application/vnd.google-apps.spreadsheet') {
            isGoogleDoc = true;
            exportMimeType = 'text/csv';
        } else if (mimeType === 'application/vnd.google-apps.presentation') {
            isGoogleDoc = true;
            exportMimeType = 'application/pdf';
        }

        try {
            let res: GaxiosResponse<Readable>;

            // Use the 'export' method for Google Docs and the 'get' method for all other file types.
            if (isGoogleDoc) {
                res = await driveClient.files.export({
                    fileId: fileId,
                    mimeType: exportMimeType!,
                }, { responseType: 'stream' }) as unknown as GaxiosResponse<Readable>;
            } else {
                res = await driveClient.files.get({
                    fileId: fileId,
                    alt: 'media',
                }, { responseType: 'stream' }) as unknown as GaxiosResponse<Readable>;
            }

            // Read the file content from the response stream into a buffer.
            const stream = res.data;
            return new Promise((resolve, reject) => {
                const chunks: Buffer[] = [];
                stream.on('data', (chunk) => chunks.push(chunk as Buffer));
                stream.on('error', reject);
                stream.on('end', () => {
                    const buffer = Buffer.concat(chunks);
                    // Convert to string for text-based formats, otherwise return the raw buffer.
                    if (isGoogleDoc || mimeType.startsWith('text/') || mimeType.includes('json') || mimeType.includes('xml')) {
                        resolve(buffer.toString('utf8'));
                    } else {
                        resolve(buffer);
                    }
                });
            });
        } catch (error: any) {
            logger.error(`GoogleDriveCrawler: Error fetching file content for '${fileId}': ${error.message}`);
            throw error;
        }
    }
}

const SourceType = 'DS';
const Type = "gdrive-crawler";
const CONFIG_FILE_NAME = "gdrive-crawler";
const DEFAULT_CONFIG = {
    folderId: "",
    authType: 'service_account',
    pageSize: 100
};

export {
    DataSource,
    SourceType,
    Type,
    CONFIG_FILE_NAME,
    DEFAULT_CONFIG
};