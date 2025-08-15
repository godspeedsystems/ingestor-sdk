import { GSDataSource, GSContext, GSStatus, logger, PlainObject } from "@godspeedsystems/core";
import * as cheerio from 'cheerio';
import { URL } from 'url';
import { IngestionData } from '../../functions/interfaces';
import axios, { AxiosInstance, AxiosResponse, isAxiosError } from 'axios';
import { IngestionTaskDefinition } from "../../functions/interfaces";

export interface HttpCrawlerConfig {
    startUrl: string; // Mandatory for a crawl
    maxDepth?: number;
    recursiveCrawling?: boolean;
    sitemapDiscovery?: boolean;
    allowedDomains?: string[];
    excludePaths?: string[];
    includePaths?: string[];
    requestTimeoutMs?: number;
    userAgent?: string;
    headers?: Record<string, string>;
}

export interface HttpCrawlerPayload extends PlainObject {
    taskDefinition: IngestionTaskDefinition;
}

export default class DataSource extends GSDataSource {
    public config: HttpCrawlerConfig;
    private axiosClient: AxiosInstance;
    private visited: Set<string>; // Tracks visited URLs to prevent infinite loops during a crawl.
    
    constructor(configWrapper: { config: HttpCrawlerConfig } | HttpCrawlerConfig) {
        super(configWrapper);

        const initialConfig = ('config' in configWrapper) ? (configWrapper as { config: HttpCrawlerConfig }).config : (configWrapper as HttpCrawlerConfig);

        this.config = {
            maxDepth: 1,
            recursiveCrawling: false,
            sitemapDiscovery: false,
            requestTimeoutMs: 30000, // 30 seconds
            userAgent: 'GodspeedCrawler/1.0',
            ...initialConfig,
        } as HttpCrawlerConfig;
        
        // FIX: Removed the mandatory startUrl check from the constructor.
        // The check is now performed at the beginning of the execute method.

        this.axiosClient = axios.create({
            timeout: this.config.requestTimeoutMs,
            headers: {
                'User-Agent': this.config.userAgent,
                ...this.config.headers,
            },
        });
        this.visited = new Set<string>();
        logger.info(`HttpCrawler initialized for URL: ${this.config.startUrl || '(not set)'}.`);
    }
    
    public async initClient(): Promise<object> {
        logger.info("HttpCrawler: Initializing HTTP client (Axios).");
        return { status: "ready" };
    }
    
    async execute(ctx: GSContext, args: PlainObject): Promise<GSStatus> {
        const initialPayload = args as HttpCrawlerPayload;
        const ingestionData: IngestionData[] = [];
        this.visited.clear();

        const config = initialPayload.taskDefinition.source.config as HttpCrawlerConfig;
        
        // FIX: The mandatory startUrl check is now performed here, at runtime.
        if (!config.startUrl) {
            logger.error("HttpCrawler: 'startUrl' is required and was not provided in the task definition.");
            return new GSStatus(false, 400, "Missing 'startUrl' configuration for standard crawl.");
        }

        if (config.sitemapDiscovery) {
            logger.info(`HttpCrawler: Sitemap discovery mode enabled. Starting from: ${config.startUrl}`);
            const sitemapUrl = await this.discoverSitemapUrl(config.startUrl);
            if (sitemapUrl) {
                await this.crawlSitemap(sitemapUrl, ingestionData, config);
            } else {
                logger.warn(`HttpCrawler: Could not discover a sitemap. Falling back to standard crawl.`);
                await this.crawlUrl(config.startUrl, 0, ingestionData, config);
            }
        } else {
            logger.info(`HttpCrawler: Standard mode - starting crawl from: ${config.startUrl}`);
            await this.crawlUrl(config.startUrl, 0, ingestionData, config);
        }

        logger.info(`HttpCrawler: Completed crawl. Fetched ${ingestionData.length} items.`);
        return new GSStatus(true, 200, "HTTP crawl successful.", {
            crawledCount: ingestionData.length,
            data: ingestionData,
        });
    }
    
    private async discoverSitemapUrl(baseUrl: string): Promise<string | null> {
        const base = new URL(baseUrl);
        const robotsUrl = `${base.protocol}//${base.hostname}/robots.txt`;

        try {
            const response = await this.axiosClient.get(robotsUrl);
            const lines = response.data.split('\n');
            for (const line of lines) {
                if (line.toLowerCase().startsWith('sitemap:')) {
                    const sitemapUrl = line.substring(line.indexOf(':') + 1).trim();
                    logger.info(`HttpCrawler: Discovered sitemap in robots.txt: ${sitemapUrl}`);
                    return sitemapUrl;
                }
            }
        } catch (error: any) {
            logger.warn(`HttpCrawler: Could not fetch or parse robots.txt at ${robotsUrl}.`);
        }
        
        const fallbackSitemapUrl = `${base.protocol}//${base.hostname}/sitemap.xml`;
        try {
            await this.axiosClient.head(fallbackSitemapUrl);
            logger.info(`HttpCrawler: Found sitemap at fallback location: ${fallbackSitemapUrl}`);
            return fallbackSitemapUrl;
        } catch (error) {
            logger.error(`HttpCrawler: No sitemap found in robots.txt or at ${fallbackSitemapUrl}.`);
            return null;
        }
    }
    
    private async crawlSitemap(sitemapUrl: string, ingestionData: IngestionData[], config: HttpCrawlerConfig): Promise<void> {
        try {
            const response = await this.axiosClient.get(sitemapUrl);
            const $ = cheerio.load(response.data, { xmlMode: true });

            if ($('sitemapindex').length > 0) {
                logger.info(`HttpCrawler: Detected sitemap index file at ${sitemapUrl}.`);
                const sitemapUrls = $('sitemap > loc').map((_, el) => $(el).text()).get();
                for (const url of sitemapUrls) {
                    await this.crawlSitemap(url, ingestionData, config); // Recurse
                }
                return;
            }

            const urls = $('urlset > url > loc').map((_, el) => $(el).text()).get();
            logger.info(`HttpCrawler: Found ${urls.length} URLs in sitemap: ${sitemapUrl}`);

            for (const url of urls) {
                if (this.isValidUrl(url, config.startUrl, config)) {
                    await this.crawlUrl(url, 0, ingestionData, config);
                }
            }
        } catch (error: any) {
            logger.error(`HttpCrawler: Failed to fetch or parse sitemap at ${sitemapUrl}: ${error.message}`);
        }
    }

    private normalizeUrl(url: string): string {
        try {
            const urlObj = new URL(url);
            if (urlObj.pathname === '') {
                urlObj.pathname = '/';
            }
            return urlObj.toString();
        } catch (e) {
            return url; // Return original if parsing fails
        }
    }

    private async crawlUrl(url: string, depth: number, ingestionData: IngestionData[], config: HttpCrawlerConfig): Promise<void> {
        const normalizedUrl = this.normalizeUrl(url);
        if (depth > (config.maxDepth || 0) || this.visited.has(normalizedUrl)) {
            return;
        }

        this.visited.add(normalizedUrl);
        logger.info(`HttpCrawler: Fetching URL: ${normalizedUrl} (Depth: ${depth})`);

        try {
            const response: AxiosResponse = await this.axiosClient.get(normalizedUrl);
            const fetchedAt = new Date();

            const item: IngestionData = {
                id: normalizedUrl,
                content: response.data,
                url: normalizedUrl,
                statusCode: response.status,
                fetchedAt: fetchedAt,
                metadata: {
                    contentType: response.headers['content-type'],
                    depth: depth,
                },
            };
            ingestionData.push(item);

            if (config.recursiveCrawling && response.headers['content-type']?.includes('text/html')) {
                const $ = cheerio.load(response.data);
                const links = $('a[href]')
                    .map((_: number, el: cheerio.Element) => $(el).attr("href")) 
                    .get()
                    .filter(link => link && this.isValidUrl(link, normalizedUrl, config))
                    .map(link => new URL(link!, normalizedUrl).href);

                for (const link of links) {
                    await this.crawlUrl(link, depth + 1, ingestionData, config);
                }
            }
        } catch (error: any) {
            logger.error(`HttpCrawler: Failed to fetch URL ${normalizedUrl}: ${error.message}`, { error });
            
            const statusCode = isAxiosError(error) ? error.response?.status || 500 : 500;

            ingestionData.push({
                id: normalizedUrl,
                content: `Error fetching: ${error.message}`,
                url: normalizedUrl,
                statusCode: statusCode,
                fetchedAt: new Date(),
                metadata: {
                    error: error.message,
                    depth: depth,
                },
            });
        }
    }
    
    private isValidUrl(link: string, baseUrl: string, config: HttpCrawlerConfig): boolean {
        try {
            const parsedUrl = new URL(link, baseUrl);
            const allowed = config.allowedDomains ? config.allowedDomains.some(domain => parsedUrl.hostname.endsWith(domain)) : true;
            const excluded = config.excludePaths ? config.excludePaths.some(path => parsedUrl.pathname.startsWith(path)) : false;
            const included = config.includePaths ? config.includePaths.some(path => parsedUrl.pathname.startsWith(path)) : true;

            return parsedUrl.protocol.startsWith('http') && allowed && !excluded && included;
        } catch (e) {
            return false;
        }
    }
}
