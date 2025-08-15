import { GSStatus } from '@godspeedsystems/core';
import axios, { AxiosError } from 'axios';
import HttpCrawlerDataSource, { HttpCrawlerConfig, HttpCrawlerPayload } from '../src/datasources/types/http-crawler';
import { IngestionData, IngestionTaskDefinition } from '../src/functions/interfaces';

// Mock the axios library
jest.mock('axios');
const mockedAxios = axios as jest.Mocked<typeof axios>;

// Mock the logger to prevent console output during tests
jest.mock('@godspeedsystems/core', () => {
    const originalModule = jest.requireActual('@godspeedsystems/core');
    return {
        ...originalModule,
        logger: {
            info: jest.fn(),
            warn: jest.fn(),
            error: jest.fn(),
            debug: jest.fn(),
        },
    };
});

// A helper to create a default payload for tests
const createMockPayload = (config: HttpCrawlerConfig): HttpCrawlerPayload => ({
  taskDefinition: {
    id: 'test-task',
    name: 'Test Crawl',
    enabled: true,
    trigger: {
        type: 'manual'
    },
    source: {
      pluginType: 'http-crawler',
      config: config,
    },
  } as IngestionTaskDefinition,
});

describe('HttpCrawler DataSource', () => {
  // Mock the axios create method to return a mock client
  const mockAxiosInstance = {
    get: jest.fn(),
    head: jest.fn(),
    post: jest.fn(),
  } as unknown as jest.Mocked<typeof axios>;

  beforeEach(() => {
    // Reset mocks before each test
    jest.clearAllMocks();
    mockedAxios.create.mockReturnValue(mockAxiosInstance);
  });

  // Test Group 1: Constructor
  describe('Constructor', () => {
   it('1. should successfully create an instance even if startUrl is not provided in constructor', () => {
  expect(() => {
    new HttpCrawlerDataSource({ config: {} as HttpCrawlerConfig });
  }).not.toThrow();
});

    it('2. should correctly apply default and custom configurations', () => {
      const crawler = new HttpCrawlerDataSource({
        config: {
          startUrl: 'https://example.com',
          maxDepth: 5,
          userAgent: 'TestBot/1.0',
        },
      });
      expect(crawler.config.maxDepth).toBe(5);
      expect(crawler.config.userAgent).toBe('TestBot/1.0');
      expect(crawler.config.recursiveCrawling).toBe(false); // Default
    });
  });

  // Test Group 2: Standard Crawl Execution
  describe('execute - Standard Crawl', () => {
    it('3. should crawl a single page when recursiveCrawling is false', async () => {
      mockAxiosInstance.get.mockResolvedValue({
        status: 200,
        data: '<html><body>Hello</body></html>',
        headers: { 'content-type': 'text/html' },
      });

      const crawler = new HttpCrawlerDataSource({ config: { startUrl: 'https://example.com' } });
      const payload = createMockPayload(crawler.config);
      const result = await crawler.execute({} as any, payload);
      
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(1);
      // FIX: Expect the normalized URL with a trailing slash.
      expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/');
      expect(result.data.crawledCount).toBe(1);
      expect(result.data.data[0].content).toContain('Hello');
    });

    it('4. should recursively crawl valid links when enabled', async () => {
      mockAxiosInstance.get
        .mockResolvedValueOnce({
          status: 200,
          data: '<html><body><a href="/page2">Page 2</a></body></html>',
          headers: { 'content-type': 'text/html' },
        })
        .mockResolvedValueOnce({
          status: 200,
          data: '<html><body>Page 2 content</body></html>',
          headers: { 'content-type': 'text/html' },
        });

      const crawler = new HttpCrawlerDataSource({
        config: { startUrl: 'https://example.com', recursiveCrawling: true, maxDepth: 1 },
      });
      const payload = createMockPayload(crawler.config);
      const result = await crawler.execute({} as any, payload);

      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(2);
      // FIX: Expect the normalized URL with a trailing slash.
      expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/');
      expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/page2');
      expect(result.data.crawledCount).toBe(2);
    });

    it('5. should not crawl deeper than maxDepth', async () => {
        mockAxiosInstance.get
        .mockResolvedValueOnce({ // Depth 0
          status: 200, data: '<html><a href="/page2">Page 2</a></html>', headers: { 'content-type': 'text/html' }
        })
        .mockResolvedValueOnce({ // Depth 1
          status: 200, data: '<html><a href="/page3">Page 3</a></html>', headers: { 'content-type': 'text/html' }
        });
        // Page 3 at depth 2 should not be fetched

      const crawler = new HttpCrawlerDataSource({
        config: { startUrl: 'https://example.com', recursiveCrawling: true, maxDepth: 1 },
      });
      const payload = createMockPayload(crawler.config);
      await crawler.execute({} as any, payload);
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(2);
    });

    it('6. should not crawl a URL that has already been visited (loop prevention)', async () => {
        // FIX: Use a specific mock implementation for this test to avoid conflicts.
        mockAxiosInstance.get.mockImplementation(async (url: string) => {
            if (url === 'https://example.com/') {
                return { status: 200, data: '<html><a href="/page2">Page 2</a></html>', headers: { 'content-type': 'text/html' } };
            }
            if (url === 'https://example.com/page2') {
                return { status: 200, data: '<html><a href="/">Home</a></html>', headers: { 'content-type': 'text/html' } };
            }
            return Promise.reject(new Error('URL not mocked'));
        });

        const crawler = new HttpCrawlerDataSource({
            config: { startUrl: 'https://example.com', recursiveCrawling: true, maxDepth: 2 },
        });
        const payload = createMockPayload(crawler.config);
        await crawler.execute({} as any, payload);
        
        // FIX: The test should pass with exactly 2 calls now.
        expect(mockAxiosInstance.get).toHaveBeenCalledTimes(2);
        expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/');
        expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/page2');
    });

    it('7. should not look for links if content is not text/html', async () => {
        mockAxiosInstance.get.mockResolvedValue({
            status: 200,
            data: '{"key": "value"}',
            headers: { 'content-type': 'application/json' },
        });
        
        const crawler = new HttpCrawlerDataSource({
            config: { startUrl: 'https://example.com/api', recursiveCrawling: true },
        });
        const payload = createMockPayload(crawler.config);
        await crawler.execute({} as any, payload);

        expect(mockAxiosInstance.get).toHaveBeenCalledTimes(1);
    });
  });

  // Test Group 3: Sitemap Crawl Execution
  describe('execute - Sitemap Crawl', () => {
    it('8. should discover sitemap from robots.txt and crawl its URLs', async () => {
      mockAxiosInstance.get
        .mockResolvedValueOnce({ // robots.txt
          status: 200,
          data: 'Sitemap: https://example.com/sitemap.xml',
        })
        .mockResolvedValueOnce({ // sitemap.xml
          status: 200,
          data: '<urlset><url><loc>https://example.com/sitemap-page</loc></url></urlset>',
        })
        .mockResolvedValueOnce({ // sitemap-page
          status: 200,
          data: 'Sitemap page content', headers: { 'content-type': 'text/html' },
        });

      const crawler = new HttpCrawlerDataSource({
        config: { startUrl: 'https://example.com', sitemapDiscovery: true },
      });
      const payload = createMockPayload(crawler.config);
      const result = await crawler.execute({} as any, payload);

      expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/robots.txt');
      expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/sitemap.xml');
      expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/sitemap-page');
      expect(result.data.crawledCount).toBe(1);
    });
    
    it('9. should use fallback /sitemap.xml if robots.txt fails', async () => {
      mockAxiosInstance.get.mockRejectedValueOnce(new Error('not found')); // robots.txt fails
      mockAxiosInstance.head.mockResolvedValueOnce({ status: 200 }); // sitemap.xml exists
      mockAxiosInstance.get.mockResolvedValueOnce({ // sitemap.xml content
          status: 200,
          data: '<urlset><url><loc>https://example.com/fallback-page</loc></url></urlset>',
      }).mockResolvedValueOnce({ status: 200, data: 'Fallback page' }); // fallback-page content

      const crawler = new HttpCrawlerDataSource({ config: { startUrl: 'https://example.com', sitemapDiscovery: true } });
      const payload = createMockPayload(crawler.config);
      await crawler.execute({} as any, payload);

      expect(mockAxiosInstance.head).toHaveBeenCalledWith('https://example.com/sitemap.xml');
      expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/sitemap.xml');
      expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/fallback-page');
    });

    it('10. should fall back to standard crawl if no sitemap is found', async () => {
      mockAxiosInstance.get.mockRejectedValueOnce(new Error('not found')); // robots.txt fails
      mockAxiosInstance.head.mockRejectedValueOnce(new Error('not found')); // sitemap.xml fails
      mockAxiosInstance.get.mockResolvedValueOnce({ status: 200, data: 'Homepage' }); // standard crawl

      const crawler = new HttpCrawlerDataSource({ config: { startUrl: 'https://example.com', sitemapDiscovery: true } });
      const payload = createMockPayload(crawler.config);
      await crawler.execute({} as any, payload);

      // FIX: Expect the normalized URL for the standard crawl.
      expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/');
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(2); // robots.txt + homepage
    });

    it('11. should correctly parse a sitemap index file and crawl sub-sitemaps', async () => {
        // FIX: Use a URL-based mock implementation for complex, multi-request tests.
        mockAxiosInstance.get.mockImplementation(async (url: string) => {
            if (url === 'https://example.com/sitemap_index.xml') {
                return { status: 200, data: `<sitemapindex><sitemap><loc>https://example.com/sitemap1.xml</loc></sitemap><sitemap><loc>https://example.com/sitemap2.xml</loc></sitemap></sitemapindex>` };
            }
            if (url === 'https://example.com/sitemap1.xml') {
                return { status: 200, data: '<urlset><url><loc>https://example.com/page1</loc></url></urlset>' };
            }
            if (url === 'https://example.com/sitemap2.xml') {
                return { status: 200, data: '<urlset><url><loc>https://example.com/page2</loc></url></urlset>' };
            }
            if (url === 'https://example.com/page1') {
                return { status: 200, data: 'Page 1' };
            }
            if (url === 'https://example.com/page2') {
                return { status: 200, data: 'Page 2' };
            }
            return Promise.reject(new Error('URL not mocked'));
        });
        
        const crawler = new HttpCrawlerDataSource({ config: { startUrl: 'https://example.com' } });
        await (crawler as any).crawlSitemap('https://example.com/sitemap_index.xml', [], crawler.config);

        expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/sitemap1.xml');
        expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/sitemap2.xml');
        expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/page1');
        expect(mockAxiosInstance.get).toHaveBeenCalledWith('https://example.com/page2');
    });
  });

  // Test Group 4: URL Validation
  describe('URL Validation (isValidUrl)', () => {
    const crawler = new HttpCrawlerDataSource({ config: { startUrl: 'https://example.com' } });
    const isValidUrl = (link: string, config: HttpCrawlerConfig) => (crawler as any).isValidUrl(link, 'https://example.com', config);

    it('12. should allow URLs from allowedDomains', () => {
      const config: HttpCrawlerConfig = { startUrl: 'https://example.com', allowedDomains: ['example.com', 'anotherexample.org'] };
      expect(isValidUrl('https://sub.example.com/path', config)).toBe(true);
      expect(isValidUrl('https://anotherexample.org', config)).toBe(true);
    });

    it('13. should block URLs from disallowed domains', () => {
      const config: HttpCrawlerConfig = { startUrl: 'https://example.com', allowedDomains: ['example.com'] };
      expect(isValidUrl('https://google.com', config)).toBe(false);
    });

    it('14. should block URLs matching excludePaths', () => {
      const config: HttpCrawlerConfig = { startUrl: 'https://example.com', excludePaths: ['/private', '/admin'] };
      expect(isValidUrl('https://example.com/private/page', config)).toBe(false);
      expect(isValidUrl('https://example.com/public/page', config)).toBe(true);
    });

    it('15. should only allow URLs matching includePaths when defined', () => {
      const config: HttpCrawlerConfig = { startUrl: 'https://example.com', includePaths: ['/blog', '/docs'] };
      expect(isValidUrl('https://example.com/blog/my-post', config)).toBe(true);
      expect(isValidUrl('https://example.com/about', config)).toBe(false);
    });
  });

  // Test Group 5: Error Handling
  describe('Error Handling', () => {
      it('16. should create an error record for a page that returns a 404', async () => {
        // FIX: Explicitly mock isAxiosError to return true for this test.
        mockedAxios.isAxiosError.mockReturnValue(true);

        const axiosError = new Error('Not Found') as AxiosError;
        axiosError.isAxiosError = true;
        axiosError.response = { status: 404, data: 'Not Found', statusText: 'Not Found', headers: {}, config: {} as any };

        mockAxiosInstance.get.mockRejectedValue(axiosError);
        const crawler = new HttpCrawlerDataSource({ config: { startUrl: 'https://example.com/notfound' } });
        const payload = createMockPayload(crawler.config);
        const result = await crawler.execute({} as any, payload);
        
        expect(result.success).toBe(true);
        expect(result.data.crawledCount).toBe(1);
        const errorRecord = result.data.data[0];
        // FIX: The test should now correctly receive 404.
        expect(errorRecord.statusCode).toBe(404);
        expect(errorRecord.content).toContain('Error fetching');
      });
      
      it('17. should gracefully handle an invalid startUrl in task definition', async () => {
        const crawler = new HttpCrawlerDataSource({ config: { startUrl: 'https://example.com' } });
        const payload = createMockPayload({ startUrl: null } as any); // Simulate invalid config
        const result = await crawler.execute({} as any, payload);
        
        expect(result.success).toBe(false);
        // FIX: The property name for the status code on GSStatus is 'code', not 'statusCode'.
        expect((result as any).code).toBe(400);
        expect(result.message).toContain("Missing 'startUrl' configuration");
      });
  });
});