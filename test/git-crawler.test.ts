// C:\Users\SOHAM\Desktop\Crawler-sdk\crawler\test\git-crawler.test.ts

import DataSource, { GitCrawlerConfig, GitCrawlerPayload, GitHubPushPayload } from '../src/datasources/types/git-crawler';
import { GSContext, GSStatus, logger } from '@godspeedsystems/core';
import simpleGit from 'simple-git';
import * as fs from 'fs/promises';
import * as os from 'os';
import { v4 as uuidv4 } from 'uuid';
import path from 'path';

// Mock external dependencies
jest.mock('simple-git');
jest.mock('fs/promises');
jest.mock('os');
jest.mock('uuid');
jest.mock('@godspeedsystems/core', () => ({
    ...jest.requireActual('@godspeedsystems/core'),
    logger: {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
    },
}));

// Mock instances and spies
const mockedSimpleGit = simpleGit as jest.Mock;
const mockedFs = fs as jest.Mocked<typeof fs>;
const mockedOs = os as jest.Mocked<typeof os>;
const mockedUuidv4 = uuidv4 as jest.Mock;
const mockedLogger = logger as jest.Mocked<typeof logger>;

// Define a mock for the simple-git fluent API
const mockGitInstance = {
    clone: jest.fn().mockResolvedValue(undefined),
    cwd: jest.fn().mockReturnThis(),
    reset: jest.fn().mockResolvedValue(undefined),
    fetch: jest.fn().mockResolvedValue(undefined),
    checkout: jest.fn().mockResolvedValue(undefined),
    pull: jest.fn().mockResolvedValue(undefined),
    remote: jest.fn().mockResolvedValue('https://github.com/test/repo.git\n'),
};

describe('GitCrawler DataSource', () => {
    const mockRepoUrl = 'https://github.com/test/repo.git';
    const mockCtx = {} as GSContext;

    beforeEach(() => {
        // Reset mocks before each test
        jest.clearAllMocks();

        // Setup default mock implementations
        mockedSimpleGit.mockReturnValue(mockGitInstance);
        mockedOs.tmpdir.mockReturnValue('/tmp');
        mockedUuidv4.mockReturnValue('mock-uuid-1234');
        mockedFs.access.mockRejectedValue(new Error('File not found')); // Default: repo does not exist
        mockedFs.rm.mockResolvedValue(undefined);
        mockedFs.mkdir.mockResolvedValue(undefined);
    });

    // 1. Test Suite for Constructor
    describe('Constructor', () => {
        

        it('should initialize with default branch and depth if not provided', () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            expect(ds.config.repoUrl).toBe(mockRepoUrl);
            expect(ds.config.branch).toBe('main');
            expect(ds.config.depth).toBe(1);
        });

        it('should accept a direct config object without a wrapper', () => {
            const ds = new DataSource({ repoUrl: mockRepoUrl, branch: 'develop', depth: 10 });
            expect(ds.config.repoUrl).toBe(mockRepoUrl);
            expect(ds.config.branch).toBe('develop');
            expect(ds.config.depth).toBe(10);
        });

        it('should correctly initialize and log info', () => {
            new DataSource({ config: { repoUrl: mockRepoUrl } });
            expect(mockedLogger.info).toHaveBeenCalledWith(
                `GitCrawler initialized for repo: ${mockRepoUrl}. Local path will be temporary.`
            );
        });
    });

    // 2. Test Suite for initClient
    describe('initClient', () => {
        it('should return a "connected" status object', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const result = await ds.initClient();
            expect(result).toEqual({ status: 'connected' });
            expect(mockedLogger.info).toHaveBeenCalledWith('GitCrawler client initialized (ready).');
        });
    });

    // 3. Test Suite for Full Crawl Scenario
    describe('execute - Full Crawl', () => {
        it('should clone repo if it does not exist and read files', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const tempPath = path.join('/tmp', 'git-crawler-mock-uuid-1234');

            // Simulate directory not existing, then reading files
            mockedFs.access.mockRejectedValueOnce(new Error('ENOENT')); // Does not exist
            mockedFs.readdir
                .mockResolvedValueOnce([{ name: 'file1.txt', isDirectory: () => false, isFile: () => true }] as any)
            mockedFs.readFile.mockResolvedValue('file content');

            // FIX: After operations, fs.access should resolve to trigger cleanup
            mockedFs.access.mockResolvedValueOnce(true as any);

            const result = await ds.execute(mockCtx, { taskDefinition: {} } as GitCrawlerPayload);

            expect(mockedFs.mkdir).toHaveBeenCalledWith(tempPath, { recursive: true });
            expect(mockGitInstance.clone).toHaveBeenCalledWith(mockRepoUrl, tempPath, { '--branch': 'main', '--depth': 1 });
            expect(mockedFs.readFile).toHaveBeenCalledWith(path.join(tempPath, 'file1.txt'), 'utf8');
            expect(result.success).toBe(true);
            expect(result.data?.data[0].content).toBe('file content');
            expect(mockedFs.rm).toHaveBeenCalledWith(tempPath, { recursive: true, force: true });
        });

        it('should pull an existing repo if it exists and remote URL matches', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });

            mockedFs.access.mockResolvedValue(true as any);
            mockedFs.stat.mockResolvedValue({ isDirectory: () => true } as any);
            mockGitInstance.remote.mockResolvedValue(`${mockRepoUrl}\n`);

            await ds.execute(mockCtx, { taskDefinition: {} } as GitCrawlerPayload);

            expect(mockGitInstance.clone).not.toHaveBeenCalled();
            expect(mockGitInstance.pull).toHaveBeenCalledWith('origin', 'main');
            expect(mockedLogger.info).toHaveBeenCalledWith(expect.stringContaining('pulling latest changes'));
        });

        it('should re-clone if existing directory has a different remote URL', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const tempPath = path.join('/tmp', 'git-crawler-mock-uuid-1234');

            mockedFs.access.mockResolvedValue(true as any);
            mockedFs.stat.mockResolvedValue({ isDirectory: () => true } as any);
            mockGitInstance.remote.mockResolvedValue('https://github.com/different/repo.git\n');

            await ds.execute(mockCtx, { taskDefinition: {} } as GitCrawlerPayload);

            expect(mockedLogger.warn).toHaveBeenCalledWith(expect.stringContaining('different repo'));
            expect(mockedFs.rm).toHaveBeenCalledWith(tempPath, { recursive: true, force: true });
            expect(mockGitInstance.clone).toHaveBeenCalledWith(mockRepoUrl, tempPath, expect.any(Object));
        });

        it('should ignore dotfiles and .git directory during recursive file search', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });

            mockedFs.readdir
                .mockResolvedValueOnce([
                    { name: 'file1.txt', isDirectory: () => false, isFile: () => true },
                    { name: '.env', isDirectory: () => false, isFile: () => true },
                    { name: '.git', isDirectory: () => true, isFile: () => false },
                    { name: 'subdir', isDirectory: () => true, isFile: () => false },
                ] as any)
                .mockResolvedValueOnce([{ name: 'file2.txt', isDirectory: () => false, isFile: () => true }] as any);
            mockedFs.readFile.mockResolvedValue('content');

            const result = await ds.execute(mockCtx, { taskDefinition: {} } as GitCrawlerPayload);

            const expectedPaths = ['file1.txt', path.join('subdir', 'file2.txt')];
            expect(result.data?.data.length).toBe(2);
            expect(result.data?.data.map((d: any) => d.metadata.filePath)).toEqual(expectedPaths);
        });

        it('should handle file read errors gracefully during a full scan', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });

            mockedFs.readdir.mockResolvedValueOnce([
                { name: 'goodfile.txt', isDirectory: () => false, isFile: () => true },
                { name: 'badfile.txt', isDirectory: () => false, isFile: () => true }
            ] as any);
            mockedFs.readFile
                .mockResolvedValueOnce('good content')
                .mockRejectedValueOnce(new Error('Read permission denied'));

            const result = await ds.execute(mockCtx, { taskDefinition: {} } as GitCrawlerPayload);

            expect(result.data?.data.length).toBe(1);
            expect(result.data?.data[0].content).toBe('good content');
            expect(mockedLogger.warn).toHaveBeenCalledWith(
                expect.stringContaining('Could not read file')
            );
            expect(mockedLogger.warn).toHaveBeenCalledWith(
                expect.stringContaining('badfile.txt')
            );
        });
    });

    // 4. Test Suite for Webhook Crawl Scenario
    describe('execute - Webhook Crawl', () => {
        const mockWebhookPayload: GitHubPushPayload = {
            ref: 'refs/heads/feature-branch',
            after: 'commit_sha_after',
            before: 'commit_sha_before',
            repository: {
                id: 1, name: 'repo', full_name: 'test/repo', html_url: mockRepoUrl
            },
            pusher: { name: 'tester', email: 'test@example.com' },
            commits: [],
            head_commit: {
                id: 'commit_sha_after',
                message: 'feat: add new feature',
                timestamp: new Date().toISOString(),
                url: '',
                author: { name: 'tester', email: 'test@example.com', username: 'tester' },
                added: ['src/new-file.ts'],
                modified: ['README.md'],
                removed: ['old-config.json']
            }
        };

        it('should successfully process a valid webhook with added, modified, and removed files', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const payload = { webhookPayload: mockWebhookPayload, taskDefinition: {} } as GitCrawlerPayload;

            mockedFs.readFile.mockResolvedValue('file content');

            const result = await ds.execute(mockCtx, payload);

            expect(mockGitInstance.reset).toHaveBeenCalledWith(['--hard', 'commit_sha_after']);
            expect(result.success).toBe(true);
            expect(result.data?.data.length).toBe(3);

            const addedFile = result.data.data.find((d: any) => d.metadata.changeType === 'added');
            const modifiedFile = result.data.data.find((d: any) => d.metadata.changeType === 'modified');
            const removedFile = result.data.data.find((d: any) => d.metadata.changeType === 'removed');

            expect(addedFile.metadata.filePath).toBe('src/new-file.ts');
            expect(modifiedFile.metadata.filePath).toBe('README.md');
            expect(removedFile.metadata.filePath).toBe('old-config.json');
            expect(removedFile.content).toBe('');
        });

        it('should handle webhook payload with no head_commit gracefully', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const payload = { webhookPayload: { ...mockWebhookPayload, head_commit: null as any }, taskDefinition: {} } as GitCrawlerPayload;

            const result = await ds.execute(mockCtx, payload);

            expect(result.success).toBe(true);
            expect(result.data.data.length).toBe(0);
            expect(mockedLogger.warn).toHaveBeenCalledWith(expect.stringContaining('No head_commit found'));
        });

        it('should create an error ingestion record if reading a changed file fails', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const payload = { webhookPayload: mockWebhookPayload, taskDefinition: {} } as GitCrawlerPayload;

            mockedFs.readFile.mockRejectedValue(new Error('Cannot read file'));

            const result = await ds.execute(mockCtx, payload);

            expect(result.data.data.length).toBe(3);
            const errorRecord = result.data.data.find((d: any) => d.statusCode === 500);
            expect(errorRecord).toBeDefined();
            expect(errorRecord.content).toContain('Error reading file: Cannot read file');
            expect(errorRecord.metadata.filePath).toBe('src/new-file.ts');
            expect(mockedLogger.warn).toHaveBeenCalledWith(
                expect.stringContaining('Could not read file')
            );
        });

        it('should correctly process a webhook where change keys (added, removed) are undefined', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });

            const payloadWithUndefinedKeys: GitHubPushPayload = {
                ref: 'refs/heads/main',
                before: 'commit_sha_abc',
                after: 'commit_sha_xyz',
                repository: { id: 1, name: 'repo', full_name: 'test/repo', html_url: mockRepoUrl },
                pusher: { name: 'tester', email: 'test@example.com' },
                commits: [],
                head_commit: {
                    id: 'commit_sha_xyz',
                    message: 'feat: modify only',
                    timestamp: new Date().toISOString(),
                    url: '',
                    author: { name: 'tester', email: 'test@example.com', username: 'tester' },
                    added: undefined,
                    modified: ['README.md'],
                    removed: undefined,
                },
            };

            // --- Explicit Mocks for this Test ---
            mockedFs.access.mockRejectedValue(new Error('ENOENT')); // Simulate repo not existing locally
            mockedFs.mkdir.mockResolvedValue(undefined);
            mockGitInstance.clone.mockResolvedValue(undefined);
            mockGitInstance.reset.mockResolvedValue(undefined);
            mockedFs.readFile.mockResolvedValue('new readme content');

            // --- Execution ---
            const result = await ds.execute(mockCtx, { webhookPayload: payloadWithUndefinedKeys, taskDefinition: {} });

            // --- Assertions ---
            expect(result.success).toBe(true);
            expect(result.data?.data).toHaveLength(1);
            expect(result.data.data[0].metadata.changeType).toBe('modified');
            expect(result.data.data[0].metadata.filePath).toBe('README.md');
        });
    });

    // 5. Test Suite for General Error Handling and Edge Cases
    describe('execute - Error Handling & Edge Cases', () => {
        it('should return a failure status for an invalid webhook payload structure', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const invalidPayload = { webhookPayload: { ref: 'ref only' }, taskDefinition: {} } as any;

            const result = await ds.execute(mockCtx, invalidPayload);

            expect(result.success).toBe(false);
            expect(result.code).toBe(500);
            expect(result.message).toContain('Invalid GitHub Push webhook payload structure');
        });

        it('should return a 500 status if a git operation fails', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const gitError = new Error('Authentication failed');
            mockGitInstance.clone.mockRejectedValue(gitError);

            const result = await ds.execute(mockCtx, { taskDefinition: {} } as GitCrawlerPayload);

            expect(result.success).toBe(false);
            expect(result.code).toBe(500);
            expect(result.message).toContain('Git operation failed: Authentication failed');
            expect(mockedLogger.error).toHaveBeenCalledWith(expect.stringContaining('Git operation failed'), expect.any(Object));
        });

        it('should handle non-Error exceptions in the main catch block', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const nonErrorObject = 'A simple string error';

            mockGitInstance.clone.mockRejectedValue(nonErrorObject);

            const result = await ds.execute(mockCtx, { taskDefinition: {} });

            expect(result.success).toBe(false);
            expect(result.code).toBe(500);
            expect(result.message).toBe('Git operation failed: Unknown error during Git operation');
            expect(result.data?.error).toBe('Unknown error during Git operation');
            expect(mockedLogger.error).toHaveBeenCalledWith(
                expect.stringContaining('Git operation failed for'),
                { error: nonErrorObject }
            );
        });

        it('should attempt cleanup even if the main operation fails', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const tempPath = path.join('/tmp', 'git-crawler-mock-uuid-1234');
            mockGitInstance.clone.mockRejectedValue(new Error('Clone failed'));

            mockedFs.access.mockResolvedValue(true as any);

            await ds.execute(mockCtx, { taskDefinition: {} } as GitCrawlerPayload);

            expect(mockedLogger.info).toHaveBeenCalledWith(
                `GitCrawler: Cleaning up temporary local path: ${tempPath}`
            );
            expect(mockedFs.rm).toHaveBeenCalledWith(tempPath, { recursive: true, force: true });
        });

        it('should log an error if cleanup fails but not throw', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const tempPath = path.join('/tmp', 'git-crawler-mock-uuid-1234');
            const cleanupError = new Error('Failed to remove directory');

            // 1. Mocks for ensureLocalRepo to succeed down the "repo exists" path
            mockedFs.access.mockResolvedValue(true as any);
            mockedFs.stat.mockResolvedValue({ isDirectory: () => true } as any);
            mockGitInstance.remote.mockResolvedValue(`${mockRepoUrl}\n`);

            // 2. Mocks for readAllFilesFromLocalPath to succeed
            mockedFs.readdir.mockResolvedValue([]);

            // 3. Mock ONLY the cleanup rm call to fail
            mockedFs.rm.mockRejectedValue(cleanupError);

            const result = await ds.execute(mockCtx, { taskDefinition: {} } as GitCrawlerPayload);

            expect(result.success).toBe(true);
            expect(mockedLogger.error).toHaveBeenCalledWith(
                `GitCrawler: Failed to clean up temporary directory ${tempPath}: ${cleanupError.message}`
            );
            expect(mockedFs.rm).toHaveBeenCalledTimes(1);
        });

        it('should re-clone if checking existing repo fails', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const tempPath = path.join('/tmp', 'git-crawler-mock-uuid-1234');
            const gitError = new Error('fatal: not a git repository');

            mockedFs.access.mockResolvedValue(true as any);
            mockedFs.stat.mockResolvedValue({ isDirectory: () => true } as any);
            mockGitInstance.remote.mockRejectedValue(gitError);

            await ds.execute(mockCtx, { taskDefinition: {} } as GitCrawlerPayload);

            expect(mockedLogger.warn).toHaveBeenCalledWith(
                expect.stringContaining('Error checking existing repo')
            );
            expect(mockedFs.rm).toHaveBeenCalledWith(tempPath, { recursive: true, force: true });
            expect(mockGitInstance.clone).toHaveBeenCalledTimes(1);
        });

        it('should remove and re-clone if the local path exists as a file', async () => {
            const ds = new DataSource({ config: { repoUrl: mockRepoUrl } });
            const tempPath = path.join('/tmp', 'git-crawler-mock-uuid-1234');

            mockedFs.access.mockResolvedValue(true as any);
            mockedFs.stat.mockResolvedValue({ isDirectory: () => false } as any);
            mockedFs.readdir.mockResolvedValue([]);

            await ds.execute(mockCtx, { taskDefinition: {} });

            expect(mockedLogger.info).toHaveBeenCalledWith(
                `GitCrawler: Local path ${tempPath} does not exist or is not a directory, cloning repo.`
            );
            expect(mockedFs.rm).toHaveBeenCalledWith(tempPath, { recursive: true, force: true });
            expect(mockGitInstance.clone).toHaveBeenCalledTimes(1);
        });
    });
});