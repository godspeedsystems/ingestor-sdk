import { jest } from '@jest/globals';
import * as fs from 'fs';
import { Readable } from 'stream';
import { GSContext, GSStatus, PlainObject } from '@godspeedsystems/core';

// This mock isolates the component from the framework's file-system access.
jest.mock('@godspeedsystems/core', () => {
  class MockGSDataSource {
    public config: any;
    constructor(configWrapper: any) { this.config = ('config' in configWrapper) ? configWrapper.config : configWrapper; }
  }
  class MockGSStatus {
    public success: boolean; public code: number; public message: string; public payload?: any;
    constructor(success: boolean, code: number, message:string, payload?: any) {
      this.success = success; this.code = code; this.message = message; this.payload = payload;
    }
  }
  return {
    GSDataSource: MockGSDataSource, GSStatus: MockGSStatus,
    logger: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
    GSContext: {}, PlainObject: {},
  };
});

const mockedFs = fs as jest.Mocked<typeof fs>;
jest.mock('fs');
import { logger } from '@godspeedsystems/core';
import DataSource from '../src/datasources/types/googledrive-crawler';

// --- Mocks for Google APIs ---
const mockAuthorize = jest.fn<(params?: any) => Promise<boolean>>();
const mockGetStartPageToken = jest.fn<(params?: any) => Promise<any>>();
const mockChangesList = jest.fn<(params?: any) => Promise<any>>();
const mockFilesList = jest.fn<(params?: any) => Promise<any>>();
const mockFilesGet = jest.fn<(params?: any) => Promise<any>>();
const mockFilesExport = jest.fn<(params?: any) => Promise<any>>();

jest.mock('googleapis', () => ({
  google: {
    auth: { JWT: jest.fn().mockImplementation(() => ({ authorize: mockAuthorize })) },
    drive: jest.fn(() => ({
      changes: { getStartPageToken: mockGetStartPageToken, list: mockChangesList },
      files: { list: mockFilesList, get: mockFilesGet, export: mockFilesExport },
    })),
  },
}));

// Helper to create a stream
const createReadableStream = (content?: string | Buffer, error?: Error): Readable => {
  const stream = new Readable();
  if (content) stream.push(content);
  if (error) process.nextTick(() => stream.emit('error', error));
  stream.push(null);
  return stream;
};

const FAKE_SERVICE_ACCOUNT_KEY = { client_email: 'test@serviceaccount.com', private_key: '-----BEGIN PRIVATE KEY-----' };
const FAKE_FOLDER_ID = 'test-folder-id-12345';
const mockContext = {} as GSContext;

describe('GoogleDriveCrawler DataSource', () => {
  beforeEach(() => { jest.clearAllMocks(); });
    
  it('constructor should handle wrapped config object', () => {
    const ds = new DataSource({ config: { folderId: 'wrapped-id' } });
    expect(ds.config.folderId).toBe('wrapped-id');
  });

  describe('initClient', () => {
    it('should initialize successfully with a key path', async () => {
      mockAuthorize.mockResolvedValue(true);
      mockedFs.readFileSync.mockReturnValue(JSON.stringify(FAKE_SERVICE_ACCOUNT_KEY));
      const ds = new DataSource({ folderId: FAKE_FOLDER_ID, serviceAccountKeyPath: '/path/to/key.json' });
      expect((await ds.initClient()).success).toBe(true);
    });

    it('should return "already initialized" if called twice', async () => {
        mockAuthorize.mockResolvedValue(true);
        const ds = new DataSource({ folderId: FAKE_FOLDER_ID, serviceAccountKey: JSON.stringify(FAKE_SERVICE_ACCOUNT_KEY) });
        await ds.initClient();
        const status = await ds.initClient();
        expect(status.message).toBe("Google Drive client already initialized.");
    });

    it('should fail if folderId is missing', async () => {
        const ds = new DataSource({});
        expect((await ds.initClient()).success).toBe(false);
    });

    it('should fail if no service key is provided', async () => {
      const ds = new DataSource({ folderId: FAKE_FOLDER_ID });
      expect((await ds.initClient()).success).toBe(false);
    });
    
    it('should fail if service account key is invalid JSON', async () => {
      const ds = new DataSource({ folderId: FAKE_FOLDER_ID, serviceAccountKey: 'not-json' });
      expect((await ds.initClient()).success).toBe(false);
    });

    it('should fail if service account key is incomplete', async () => {
      const ds = new DataSource({ folderId: FAKE_FOLDER_ID, serviceAccountKey: JSON.stringify({ client_email: 'email' }) });
      expect((await ds.initClient()).success).toBe(false);
    });

    it('should fail if authorization fails', async () => {
      mockAuthorize.mockRejectedValue(new Error('Auth failed'));
      const ds = new DataSource({ folderId: FAKE_FOLDER_ID, serviceAccountKey: JSON.stringify(FAKE_SERVICE_ACCOUNT_KEY) });
      expect((await ds.initClient()).success).toBe(false);
    });
  });

  describe('execute', () => {
    let ds: DataSource;
    beforeEach(() => {
      mockAuthorize.mockResolvedValue(true);
      ds = new DataSource({ folderId: FAKE_FOLDER_ID, serviceAccountKey: JSON.stringify(FAKE_SERVICE_ACCOUNT_KEY) });
      mockFilesGet.mockImplementation(() => Promise.resolve({ data: createReadableStream('content') }));
      mockFilesExport.mockImplementation(() => Promise.resolve({ data: createReadableStream('content') }));
    });

    it('should stop execution if client fails to initialize', async () => {
        const dsWithBadConfig = new DataSource({ folderId: '' });
        const result = await dsWithBadConfig.execute(mockContext, {});
        expect(result.success).toBe(false);
    });

    describe('Full Scan Mode', () => {
      it('should perform a multi-page scan', async () => {
        mockFilesList.mockImplementation((params: any) => {
          if (params.pageToken === 'page2') return Promise.resolve({ data: { files: [{ id: 'f2', name: 'F2' }], nextPageToken: null } });
          return Promise.resolve({ data: { files: [{ id: 'f1', name: 'F1' }], nextPageToken: 'page2' } });
        });
        const result = await ds.execute(mockContext, { taskDefinition: {} });
        expect((result as any).payload.crawledCount).toBe(2);
      });

      it('should handle API failure when listing files', async () => {
        mockFilesList.mockRejectedValue(new Error('API List Error'));
        const result = await ds.execute(mockContext, { taskDefinition: {} });
        expect(result.success).toBe(false);
        expect(result.message).toContain('API List Error');
      });

      it('should create a failure record when file content fetch fails', async () => {
        mockFilesList.mockResolvedValue({ data: { files: [{ id: 'bad', name: 'Bad File' }] } });
        mockFilesGet.mockRejectedValue(new Error('Cannot download'));
        const result = await ds.execute(mockContext, { taskDefinition: {} });
        const record = (result as any).payload.data[0];
        expect(record.statusCode).toBe(500);
      });
    });

    describe('Webhook Mode', () => {
      it('should get a startPageToken on the first run', async () => {
        mockGetStartPageToken.mockResolvedValue({ data: { startPageToken: 'token123' } });
        const result = await ds.execute(mockContext, { taskDefinition: {}, webhookPayload: {} });
        expect((result as any).payload.startPageToken).toBe('token123');
      });

      it('should process a mix of changes (upsert, remove, trash, skip)', async () => {
        mockChangesList.mockResolvedValue({
          data: {
            changes: [
              { fileId: 'upserted', file: { id: 'upserted', name: 'Updated.txt', parents: [FAKE_FOLDER_ID] } },
              { fileId: 'deleted', removed: true },
              { fileId: 'trashed', file: { id: 'trashed', trashed: true } },
              { fileId: 'skipped', file: { parents: ['other_folder'] } },
            ]
          }
        });
        const result = await ds.execute(mockContext, { taskDefinition: {}, webhookPayload: {}, startPageToken: 'token456' });
        expect((result as any).payload.crawledCount).toBe(3);
      });

      it('should create a failure record for a failed content fetch', async () => {
        // FIX: Added `parents` array to the mock to ensure the file is processed.
        mockChangesList.mockResolvedValue({ data: { changes: [{ fileId: 'bad', file: {id: 'bad', name: 'Bad', parents: [FAKE_FOLDER_ID] }}] } });
        mockFilesGet.mockRejectedValue(new Error('Cannot get content'));
        const result = await ds.execute(mockContext, { taskDefinition: {}, webhookPayload: {}, startPageToken: 't456'});
        const record = (result as any).payload.data[0];
        expect(record.statusCode).toBe(500);
        expect(record.metadata.error).toBe('Cannot get content');
      });
    });
  });

  describe('_getFileContent', () => {
    let ds: DataSource;
    let mockDriveClient: any;
    beforeAll(() => {
      ds = new DataSource({ folderId: FAKE_FOLDER_ID });
      mockDriveClient = { files: { get: mockFilesGet, export: mockFilesExport } };
    });

    it('should handle Google Docs by exporting as text', async () => {
        mockFilesExport.mockResolvedValue({ data: createReadableStream('doc content') });
        const content = await (ds as any)._getFileContent(mockDriveClient, 'doc_id', 'application/vnd.google-apps.document');
        expect(content).toBe('doc content');
    });

    it('should handle Google Slides (testing current string conversion bug)', async () => {
        const pdfBuffer = Buffer.from('%PDF-1.4-fake-pdf-content');
        mockFilesExport.mockResolvedValue({ data: createReadableStream(pdfBuffer) });
        const content = await (ds as any)._getFileContent(mockDriveClient, 'slide_id', 'application/vnd.google-apps.presentation');
        expect(content).toEqual(pdfBuffer.toString('utf8'));
    });

    it('should return a buffer for binary file types', async () => {
      const buffer = Buffer.from([1, 2, 3]);
      mockFilesGet.mockResolvedValue({ data: createReadableStream(buffer) });
      const content = await (ds as any)._getFileContent(mockDriveClient, 'file_id', 'image/jpeg');
      expect(content).toBeInstanceOf(Buffer);
    });
    
    it('should reject if the download stream emits an error', async () => {
        const streamError = new Error('Network timeout');
        mockFilesGet.mockResolvedValue({ data: createReadableStream(undefined, streamError) });
        await expect((ds as any)._getFileContent(mockDriveClient, 'file_id', 'image/jpeg')).rejects.toThrow(streamError);
    });
    // Add this entire block to your existing test file to improve branch coverage

describe('Coverage-focused Tests for Edge Cases', () => {
    let ds: DataSource;
    
    beforeEach(() => {
        mockAuthorize.mockResolvedValue(true);
        ds = new DataSource({ folderId: FAKE_FOLDER_ID, serviceAccountKey: JSON.stringify(FAKE_SERVICE_ACCOUNT_KEY) });
        mockFilesGet.mockImplementation(() => Promise.resolve({ data: createReadableStream('content') }));
    });

    /**
     * NEW TEST for Branch Coverage
     * Covers the other path in the constructor's ternary operator.
     */
    it('constructor should handle a direct config object', () => {
        const dsWithDirectConfig = new DataSource({ folderId: 'direct-id' });
        expect(dsWithDirectConfig.config.folderId).toBe('direct-id');
    });

    /**
     * NEW TEST for Branch Coverage
     * Covers the `else if (initialPayload?.nextPageToken)` branch in webhook mode.
     */
    it('should handle a webhook call using nextPageToken', async () => {
        mockChangesList.mockResolvedValue({ data: { changes: [] } });
        await ds.execute(mockContext, { taskDefinition: {}, webhookPayload: {}, nextPageToken: 'continueToken' });
        expect(logger.info).toHaveBeenCalledWith(expect.stringContaining('delta sync from nextPageToken'));
    });

    /**
     * NEW TEST for Branch Coverage
     * Covers the `file.size` ternary operator (`? :`) in Full Scan mode.
     */
    it('should handle files with a missing size property', async () => {
        mockFilesList.mockResolvedValue({ data: { files: [{ id: 'file1', name: 'File No Size', size: null }] } });
        const result = await ds.execute(mockContext, { taskDefinition: {} });
        const fileMetadata = (result as any).payload.data[0].metadata;
        expect(fileMetadata.fileSize).toBe(0);
    });

    /**
     * NEW TEST for Branch Coverage
     * Covers the URL fallback logic (`||`) in Full Scan mode.
     */
    it('should use webContentLink as a fallback for the URL', async () => {
        mockFilesList.mockResolvedValue({ data: { files: [{ id: 'file1', name: 'File 1', webViewLink: null, webContentLink: 'http://web.content.link' }] } });
        const result = await ds.execute(mockContext, { taskDefinition: {} });
        const fileData = (result as any).payload.data[0];
        expect(fileData.url).toBe('http://web.content.link');
    });

    /**
     * NEW TEST for Branch Coverage
     * Covers the second part of `if (fileId && file.name)` in Full Scan mode.
     */
    it('should skip files with a missing name property', async () => {
        mockFilesList.mockResolvedValue({ data: { files: [{ id: 'file1', name: 'Good File' }, { id: 'file2', name: null }] } });
        const result = await ds.execute(mockContext, { taskDefinition: {} });
        // The file with the null name should be skipped, so only 1 file is processed.
        expect((result as any).payload.crawledCount).toBe(1);
    });

      
     
    it('should correctly process a standard trashed file', async () => {
        // --- Setup ---
        // Mock a file that is in the trash and has all its properties
        const trashedFile = {
            id: 'trashed-file-123',
            name: 'Old Report.docx',
            trashed: true,
            webViewLink: 'https://drive.google.com/file/d/trashed-file-123/view',
            webContentLink: 'https://drive.google.com/uc?id=trashed-file-123',
            size: '4096',
            createdTime: '2025-08-11T10:00:00Z',
            modifiedTime: '2025-08-11T11:00:00Z',
            parents: [FAKE_FOLDER_ID]
        };
        mockFilesList.mockResolvedValue({ data: { files: [trashedFile] } });

        // --- Execute ---
        const result = await ds.execute(mockContext, { taskDefinition: {} });

        // --- Assert ---
        const payload = (result as any).payload;
        expect(payload.crawledCount).toBe(1); // Ensure one file was processed

        const record = payload.data[0];
        // Check the core properties
        expect(record.id).toBe('trashed-file-123');
        expect(record.content).toBe(''); // Content should be empty for deletions
        expect(record.url).toBe(trashedFile.webViewLink); // Should use the first available link

        // Check the metadata
        const metadata = record.metadata;
        expect(metadata.changeType).toBe('DELETE');
        expect(metadata.ingestionType).toBe('gdrive_full_scan_trash');
        expect(metadata.fileName).toBe('Old Report.docx');
        expect(metadata.fileSize).toBe(4096);
        expect(metadata.trashed).toBe(true);

        // Check that the correct log message was written
        expect(logger.info).toHaveBeenCalledWith(
            expect.stringContaining("Processed trashed file 'Old Report.docx'")
        );
    });

    /**
     * Test 2: Verifies the fallback logic for a trashed file with sparse data.
     * This checks that default values are used when metadata properties are null.
     */
    it('should handle a trashed file with missing metadata using fallbacks', async () => {
        // --- Setup ---
        const sparseTrashedFile = {
            id: 'sparse-file-456',
            // FIX: Provide a valid name to pass the `if (fileId && file.name)` check.
            name: 'File With Missing Parts',
            trashed: true,
            webViewLink: null,        // Will test '||' fallback
            webContentLink: null,     // Will test '||' fallback
            size: null,               // Will use '?:' fallback
            parents: undefined,       // will use '??' fallback
            createdTime: null,
            modifiedTime: null,
        };
        mockFilesList.mockResolvedValue({ data: { files: [sparseTrashedFile] } });

        // --- Execute ---
        const result = await ds.execute(mockContext, { taskDefinition: {} });
        
        // --- Assert ---
        // The record will now exist because the file was not skipped.
        const record = (result as any).payload.data[0];
        const metadata = record.metadata;

        // Check that the fallback values were used correctly
        expect(metadata.fileName).toBe('File With Missing Parts'); // Uses the real name
        expect(metadata.fileSize).toBe(0);
        expect(metadata.parents).toEqual([]); // Should default to an empty array
        
        // Check that the final fallback URL was constructed
        expect(record.url).toBe('https://drive.google.com/file/d/sparse-file-456/view');
    });
});
  });
});