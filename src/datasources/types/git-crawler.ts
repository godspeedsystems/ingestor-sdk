// C:\Users\SOHAM\Desktop\test-crawler\CRAWLER-SDK\src\datasources\types\git-crawler.ts

import { GSContext, GSDataSource, GSStatus, logger, PlainObject } from "@godspeedsystems/core";
import simpleGit, { SimpleGit, CloneOptions } from "simple-git";
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';
import { v4 as uuidv4 } from 'uuid';
import { IngestionData } from '../../functions/interfaces';
import { IngestionTaskDefinition } from "../../functions/interfaces";

export interface GitHubCommit {
    id: string;
    message: string;
    timestamp: string;
    url: string;
    author: { name: string; email: string; username: string };
    added?: string[];
    removed?: string[];
    modified?: string[];
}

export interface GitHubRepository {
    id: number;
    name: string;
    full_name: string;
    html_url: string;
}

export interface GitHubPushPayload {
    ref: string;
    before: string;
    after: string;
    repository: GitHubRepository;
    pusher: { name: string; email: string };
    commits: GitHubCommit[];
    head_commit: GitHubCommit;
}

export interface GitCrawlerConfig {
    repoUrl: string;
    branch?: string;
    depth?: number;
}

export interface GitCrawlerPayload extends PlainObject {
    webhookPayload?: GitHubPushPayload | { zen: string; hook_id: number; hook: any; repository: any; sender: any; }; // Allow for ping payload structure
    taskDefinition: IngestionTaskDefinition;
}

export default class DataSource extends GSDataSource {
    private git: SimpleGit = simpleGit();
    public config: GitCrawlerConfig;

    constructor(configWrapper: { config: GitCrawlerConfig } | GitCrawlerConfig) {
        super(configWrapper);

        const extractedConfig = ('config' in configWrapper) ? (configWrapper as { config: GitCrawlerConfig }).config : (configWrapper as GitCrawlerConfig);

        // Set default values for optional configuration fields
        this.config = {
            branch: "main",
            depth: 1,
            ...extractedConfig,
        } as GitCrawlerConfig;

        // FIX: Removed repoUrl validation from constructor.
        // It will now be validated in the execute method when the crawler is actually run.

        logger.info(`GitCrawler initialized for repo: ${this.config.repoUrl || 'Not specified (lazy init expected)'}. Local path will be temporary.`);
    }

    public async initClient(): Promise<object> {
        logger.info("GitCrawler client initialized (ready).");
        return { status: "connected" };
    }

    async execute(ctx: GSContext, args: PlainObject): Promise<GSStatus> {
        const initialPayload = args as GitCrawlerPayload;
        const fetchedAt = new Date();
        const baseTempDir = os.tmpdir();
        // Create a unique temporary directory for each execution to avoid conflicts.
        const tempLocalPath = path.join(baseTempDir, `git-crawler-${uuidv4()}`);

        try {
            // FIX: Add repoUrl validation here, returning a GSStatus if missing.
            // This ensures validation occurs only when the crawler is actively executed.
            if (!this.config.repoUrl) {
                logger.error("GitCrawler: 'repoUrl' is required in the task definition for execution.");
                return new GSStatus(false, 400, "Missing 'repoUrl' configuration for Git crawl.");
            }

            // This block handles incremental crawls triggered by a GitHub push webhook.
            if (initialPayload?.webhookPayload) {
                logger.info(`GitCrawler: Operating in webhook mode.`);
                const githubPayload = initialPayload.webhookPayload;

                // FIX: Handle GitHub 'ping' event gracefully
                if ('zen' in githubPayload && 'hook_id' in githubPayload) {
                    logger.info(`GitCrawler: Received GitHub 'ping' event for hook ID ${githubPayload.hook_id}. No data ingestion performed.`);
                    return new GSStatus(true, 200, "GitHub 'ping' event processed successfully (no data ingested).");
                }

                // Now, proceed with validation for actual push/pull_request events
                if (!('ref' in githubPayload) || !('repository' in githubPayload) || !Array.isArray(githubPayload.commits)) {
                    throw new Error("Invalid GitHub Push webhook payload structure.");
                }
                // Cast to GitHubPushPayload after validation to ensure type safety for subsequent access
                const pushPayload = githubPayload as GitHubPushPayload; 
                
                logger.info(`GitCrawler: Processing GitHub push for repo '${pushPayload.repository.full_name}', ref: '${pushPayload.ref}', after commit: ${pushPayload.after}`);

                const webhookBranch = pushPayload.ref.replace('refs/heads/', '');

                await this.ensureLocalRepo(pushPayload.repository.html_url, webhookBranch, { ...this.config, localPath: tempLocalPath });
                
                // Reset the local repository to the exact state of the commit from the webhook.
                await this.git.cwd(tempLocalPath).reset(['--hard', pushPayload.after]);
                logger.info(`GitCrawler: Reset temporary local repo to commit ${pushPayload.after}`);

                const ingestionData: IngestionData[] = [];
                const headCommit = pushPayload.head_commit;
                if (headCommit) {
                    const changedFiles = [
                        ...(headCommit.added || []).map(f => ({ path: f, type: 'added' })),
                        ...(headCommit.modified || []).map(f => ({ path: f, type: 'modified' }))
                    ];

                    for (const fileChange of changedFiles) {
                        const fullPath = path.join(tempLocalPath, fileChange.path);
                        try {
                            const content = await fs.readFile(fullPath, 'utf8');
                            ingestionData.push({
                                id: `${pushPayload.repository.full_name}-${fileChange.path}`,
                                content: content,
                                url: `${pushPayload.repository.html_url}/blob/${webhookBranch}/${fileChange.path}`,
                                statusCode: 200,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    filePath: fileChange.path,
                                    changeType: fileChange.type,
                                    commitSha: headCommit.id, 
                                    commitMessage: headCommit.message,
                                    repo: pushPayload.repository.full_name,
                                    branch: webhookBranch,
                                    pusher: pushPayload.pusher.name,
                                    commitAuthor: headCommit.author.name
                                }
                            });
                        } catch (readError: any) {
                            logger.warn(`GitCrawler: Could not read file ${fullPath} for ingestion (added/modified): ${readError.message}`);
                            ingestionData.push({
                                id: `${pushPayload.repository.full_name}-${fileChange.path}-error`,
                                content: `Error reading file: ${readError.message}`,
                                statusCode: 500,
                                fetchedAt: fetchedAt,
                                metadata: {
                                    filePath: fileChange.path,
                                    changeType: fileChange.type,
                                    commitSha: headCommit.id,
                                    repo: pushPayload.repository.full_name,
                                    branch: webhookBranch,
                                    error: readError.message
                                }
                            });
                        }
                    }

                    for (const removedFile of (headCommit.removed || [])) {
                        ingestionData.push({
                            id: `${pushPayload.repository.full_name}-${removedFile}`,
                            content: '',
                            url: `${pushPayload.repository.html_url}/blob/${webhookBranch}/${removedFile}`,
                            statusCode: 200,
                            fetchedAt: fetchedAt,
                            metadata: {
                                filePath: removedFile,
                                changeType: 'removed',
                                commitSha: headCommit.id,
                                commitMessage: headCommit.message,
                                repo: pushPayload.repository.full_name,
                                branch: webhookBranch,
                                pusher: githubPayload.pusher.name,
                                commitAuthor: headCommit.author.name
                            }
                        });
                    }
                } else {
                    logger.warn(`GitCrawler: No head_commit found in GitHub push payload for detailed file changes.`);
                }

                logger.info(`GitCrawler: Processed webhook, generated ${ingestionData.length} IngestionData items.`);
                return new GSStatus(true, 200, "Webhook processed and files ingested.", { data: ingestionData });

            }
            // This block handles a full scan of the entire repository.
            else {
                logger.info(`GitCrawler: Operating in standard (full clone) mode.`);
                const { repoUrl, branch, depth } = this.config;

                // repoUrl is already checked at the beginning of execute.

                await this.ensureLocalRepo(repoUrl, branch, { ...this.config, localPath: tempLocalPath });
                const allFilesData = await this.readAllFilesFromLocalPath(tempLocalPath, repoUrl, fetchedAt, branch);
                logger.info(`GitCrawler: Cloned/Pulled and read ${allFilesData.length} files from ${repoUrl}.`);

                return new GSStatus(true, 200, "Repository cloned/pulled and files read successfully", {
                    path: tempLocalPath,
                    branch: branch,
                    repoUrl: repoUrl,
                    data: allFilesData
                });
            }
        } catch (error: any) {
            const errMessage = error instanceof Error ? error.message : "Unknown error during Git operation";
            logger.error(`Git operation failed for ${this.config.repoUrl}: ${errMessage}`, { error: error });
            return new GSStatus(false, 500, `Git operation failed: ${errMessage}`, {
                repoUrl: this.config.repoUrl,
                localPath: tempLocalPath,
                error: errMessage,
            });
        } finally {
            // This block ensures the temporary directory is always removed after execution, preventing disk space leaks.
            try {
                if (await fs.access(tempLocalPath).then(() => true).catch(() => false)) {
                    logger.info(`GitCrawler: Cleaning up temporary local path: ${tempLocalPath}`);
                    await fs.rm(tempLocalPath, { recursive: true, force: true });
                }
            } catch (cleanupError: any) {
                logger.error(`GitCrawler: Failed to clean up temporary directory ${tempLocalPath}: ${cleanupError.message}`);
            }
        }
    }

    /**
     * Ensures a local clone of the repository exists at the specified path.
     * It handles cloning, updating an existing clone, or replacing an incorrect one.
     */
    private async ensureLocalRepo(repoUrl: string, branch: string | undefined, config: GitCrawlerConfig & { localPath: string }): Promise<void> {
        const repoExists = await fs.access(config.localPath).then(() => true).catch(() => false);
        if (repoExists && (await fs.stat(config.localPath)).isDirectory()) {
            try {
                // If a repo exists, check if it has the correct remote URL.
                const currentRemote = await this.git.cwd(config.localPath).remote(['get-url', 'origin']);
                if (currentRemote && currentRemote.trim() === repoUrl) {
                    // If it's the correct repo, reset and pull the latest changes.
                    logger.info(`GitCrawler: Local repo ${config.localPath} exists and matches URL, pulling latest changes.`);
                    await this.git.cwd(config.localPath).reset(['--hard']);
                    await this.git.cwd(config.localPath).fetch('origin', branch || 'main');
                    await this.git.cwd(config.localPath).checkout(branch || 'main');
                    await this.git.cwd(config.localPath).pull('origin', branch || 'main');
                } else {
                    // If the path is occupied by something else, remove it and clone fresh.
                    logger.warn(`GitCrawler: Local path ${config.localPath} exists but is a different repo or not a git repo. Attempting to remove and clone.`);
                    await fs.rm(config.localPath, { recursive: true, force: true });
                    await this.cloneRepo(repoUrl, branch, config.localPath, config.depth);
                }
            } catch (gitError: any) {
                // If checking the existing repo fails, assume it's corrupted, remove and re-clone.
                logger.warn(`GitCrawler: Error checking existing repo at ${config.localPath}: ${gitError.message}. Attempting to remove and re-clone.`);
                await fs.rm(config.localPath, { recursive: true, force: true });
                await this.cloneRepo(repoUrl, branch, config.localPath, config.depth);
            }
        } else {
            // If the path doesn't exist, create it and clone the repo.
            logger.info(`GitCrawler: Local path ${config.localPath} does not exist or is not a directory, cloning repo.`);
            await fs.mkdir(config.localPath, { recursive: true });
            await this.cloneRepo(repoUrl, branch, config.localPath, config.depth);
        }
    }

    /**
     * Clones a repository with the specified options for branch and depth.
     */
    private async cloneRepo(repoUrl: string, branch: string | undefined, localPath: string, depth: number | undefined): Promise<void> {
        const cloneOptions: CloneOptions = {};
        if (branch !== undefined) {
            cloneOptions['--branch'] = branch;
        }
        if (depth !== undefined) {
            cloneOptions['--depth'] = depth;
        }
        await this.git.clone(repoUrl, localPath, cloneOptions);
    }

    /**
     * Recursively reads all files from a directory and formats them into IngestionData objects.
     */
    private async readAllFilesFromLocalPath(basePath: string, repoUrl: string, fetchedAt: Date, branch: string | undefined): Promise<IngestionData[]> {
        const ingestionData: IngestionData[] = [];
        const files = await this.getFilesRecursive(basePath, basePath);
        const repoBranch = branch || 'main';

        for (const filePath of files) {
            const fullPath = path.join(basePath, filePath);
            try {
                const content = await fs.readFile(fullPath, 'utf8');
                ingestionData.push({
                    id: `${repoUrl}-${filePath}`,
                    content: content,
                    url: `${repoUrl}/blob/${repoBranch}/${filePath}`,
                    statusCode: 200,
                    fetchedAt: fetchedAt,
                    metadata: {
                        filePath: filePath,
                        changeType: 'full_scan',
                        repo: repoUrl,
                        branch: repoBranch
                    }
                });
            } catch (readError: any) {
                logger.warn(`GitCrawler: Could not read file ${fullPath} during full scan: ${readError.message}`);
            }
        }
        return ingestionData;
    }

    /**
     * Recursively walks a directory to build a list of all file paths, ignoring dotfiles.
     */
    private async getFilesRecursive(dir: string, rootDir: string): Promise<string[]> {
        let files: string[] = [];
        const entries = await fs.readdir(dir, { withFileTypes: true });

        for (const entry of entries) {
            const fullPath = path.join(dir, entry.name);
            // Ignore the .git directory and any other hidden files/folders.
            if (entry.name === '.git' || entry.name.startsWith('.')) {
                continue;
            }

            if (entry.isDirectory()) {
                files = files.concat(await this.getFilesRecursive(fullPath, rootDir));
            } else {
                files.push(path.relative(rootDir, fullPath));
            }
        }
        return files;
    }
}

const SourceType = "DS";
const Type = "git-crawler";
const CONFIG_FILE_NAME = "git-crawler";
const DEFAULT_CONFIG = {
    repoUrl: "",
    branch: "main",
    depth: 1,
};

export {
    DataSource,
    SourceType,
    Type,
    CONFIG_FILE_NAME,
    DEFAULT_CONFIG
};