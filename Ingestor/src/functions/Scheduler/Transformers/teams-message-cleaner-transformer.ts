// src/functions/Scheduler/Transformers/teams-message-cleaner-transformer.ts

import { IngestionData } from '../interfaces';
import { logger } from '@godspeedsystems/core';
import * as cheerio from 'cheerio'; // Ensure cheerio is imported

/**
 * Transforms raw Teams message data (assumed to be HTML content) into clean plain text.
 * It removes HTML tags, normalizes whitespace, and extracts relevant text content.
 *
 * @param rawData An array of raw data items, where each item's content is expected to be HTML.
 * @returns A Promise resolving to an array of IngestionData items with cleaned text content.
 */
const teamsMessageCleanerTransformer = async (rawData: any[]): Promise<IngestionData[]> => {
    logger.info(`TeamsMessageCleanerTransformer: Processing ${rawData.length} items.`);
    const transformedData: IngestionData[] = [];

    for (const item of rawData) {
        if (typeof item.content !== 'string' || !item.content.includes('<html') && !item.content.includes('<body')) {
            logger.warn(`TeamsMessageCleanerTransformer: Item ${item.id} content is not HTML or is not a string. Skipping transformation.`);
            transformedData.push(item); // Pass through unchanged if not HTML
            continue;
        }

        try {
            const $ = cheerio.load(item.content);

            // FIX: Select the body or html element to get its text content
            // This correctly targets the text within the main content area of the HTML.
            const cleanedContent = $('body').text().replace(/\s+/g, ' ').trim(); 

            transformedData.push({
                ...item, // Keep existing properties
                content: cleanedContent, // Replace with cleaned text
                metadata: {
                    ...item.metadata,
                    ingestionType: 'teams_message_cleaned',
                    originalContentType: item.metadata?.contentType || 'text/html',
                    // You might add a flag here like 'textExtracted: true'
                }
            });
            logger.debug(`TeamsMessageCleanerTransformer: Cleaned content for item ${item.id}.`);
        } catch (error: any) {
            logger.error(`TeamsMessageCleanerTransformer: Failed to clean content for item ${item.id}: ${error.message}`, { error });
            // Push original item with an error flag in metadata
            transformedData.push({
                ...item,
                metadata: {
                    ...item.metadata,
                    ingestionType: 'teams_message_cleaning_failed',
                    error: error.message,
                }
            });
        }
    }

    logger.info(`TeamsMessageCleanerTransformer: Finished processing. Returned ${transformedData.length} items.`);
    return transformedData;
};

export default teamsMessageCleanerTransformer;
