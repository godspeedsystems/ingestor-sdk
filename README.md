Crawler SDK
A robust and extensible SDK for ingesting data from various external sources such as Git repositories, Google Drive, and HTTP endpoints. Designed to be stateless, the crawlers integrate seamlessly with an external Scheduler SDK for orchestration and task management.

Table of Contents
Introduction

Features

Architecture Overview

Installation

Usage

General Concept

Configuration

Output Format (IngestionData)

Crawler Types

6.1. Git Crawler

6.2. Google Drive Crawler

6.3. HTTP Crawler

Webhook Processing & Utilities

Testing

License

1. Introduction
The Crawler SDK is a collection of specialized data source components built with TypeScript and Node.js. Its primary function is to efficiently extract and standardize data from diverse external systems. Designed with statelessness in mind, these crawlers are intended to be invoked by an external Scheduler SDK, which manages task orchestration, scheduling, and persistent state.

This SDK provides modular, testable, and extensible components for integrating various data sources into a larger data ingestion pipeline.

2. Features
Multi-Source Data Ingestion: Supports Git repositories, Google Drive, and HTTP/HTTPS endpoints.

Stateless Design: Crawlers are stateless, making them highly scalable and resilient.

Full Data Scans: Capability to perform complete initial data extractions.

Incremental Data Syncs (Delta Sync): For supported sources (e.g., Google Drive), fetches only changes since the last successful ingestion using continuation tokens.

Webhook-Driven Triggers: Processes incoming webhooks (e.g., GitHub push events, Google Drive change notifications) to trigger targeted ingestion.

Flexible Configuration: Configurable via YAML files (for DataSource initialization) and dynamic payloads (for task execution).

Standardized Output: Transforms extracted data into a consistent IngestionData format.

Temporary Local Storage: Git operations utilize temporary local paths, ensuring no persistent local data is left behind.

Robust Error Handling & Logging: Detailed logging and graceful error management during all stages.

3. Architecture Overview
The Crawler SDK operates as a set of stateless workers within a larger, loosely coupled architecture. It is designed to be invoked directly by an external Scheduler SDK.

Key Principles:

Statelessness: Each crawler instance created for a task execution is ephemeral. It does not maintain any long-term internal state. All necessary information (configuration, authentication, continuation tokens, webhook payloads) is passed to it at the point of invocation.

Single Responsibility: Each crawler (git-crawler.ts, gdrive-crawler.ts, http-crawler.ts) focuses solely on data extraction from its specific source.

Separation of Concerns: Webhook management (registration/deregistration) and payload processing are handled by dedicated utilities within the SDK, often invoked by the Scheduler SDK.

Interaction Flow:

The Scheduler SDK (the orchestrator) determines when a task needs to run (e.g., cron schedule, webhook event, manual trigger).

The Scheduler SDK retrieves the full taskDefinition (including source configuration, authentication details, and latest continuation tokens) from its persistent database.

The Scheduler SDK directly instantiates a new instance of the relevant Crawler SDK component (e.g., new GitCrawlerDataSource(...)) and invokes its execute() method with a comprehensive initialPayload.

The invoked Crawler SDK instance performs the data extraction from the external source based only on the provided payload.

Upon completion, the Crawler SDK returns the IngestionData and a GSStatus (including any updated continuation tokens) back to the Scheduler SDK.

The Scheduler SDK then persists these updated tokens and the task status in its database for future runs.

This architecture ensures that the Crawler SDK remains lightweight, highly scalable, and resilient, as any instance can process any valid task request without needing prior history or complex state synchronization.

4. Installation
To use the Crawler SDK, you need to set up your Node.js environment and install the required dependencies.

4.1. Prerequisites
Node.js (LTS version recommended)

npm (Node Package Manager) or pnpm

4.2. Dependency Installation
Navigate to your project's root directory (where package.json is located) in your terminal and install the dependencies:

# Install runtime dependencies
npm install @godspeedsystems/core simple-git axios uuid googleapis google-auth-library cheerio

# Install development dependencies (for testing, TypeScript compilation)
npm install --save-dev jest ts-jest @types/jest ts-node @types/axios @types/cheerio

Note on @types packages: Some libraries (like simple-git, googleapis, google-auth-library) bundle their TypeScript definitions directly, so explicit @types/ packages for them might not be necessary and can sometimes cause conflicts (E404 errors during install). The command above includes only the @types packages typically required.

5. Usage
The Crawler SDK components are designed to be invoked by an external orchestrator (like the Scheduler SDK).

5.1. General Concept
Your Scheduler SDK will typically:

Load a taskDefinition from its database.

Based on taskDefinition.source.pluginType, dynamically determine which DataSource class to use (e.g., git-crawler.ts's DataSource).

Create an instance: const crawlerInstance = new DataSource({ config: taskDefinition.source.config });

Initialize the crawler's client: await crawlerInstance.initClient();

Execute the crawl: const result = await crawlerInstance.execute(context, initialPayload);

Process result.data.data (the IngestionData) and result.data.startPageToken/nextPageToken for persistence.

5.2. Configuration
Each crawler type is configured via a GitCrawlerConfig, GoogleDriveCrawlerConfig, or HttpCrawlerConfig object. These configurations are typically defined in YAML files (e.g., config/datasources/git-crawler.yaml) and are used primarily for the initialization of the DataSource type when the system starts up. They serve as a reference and guide for developers to understand the available configuration parameters for each crawler.

Crucially, users do not have to explicitly create a separate YAML file for each new data source instance or task they register. Instead, when an Administrator/User defines a new ingestion task (e.g., via an API call to the Scheduler SDK), the specific configuration for that task's source is provided as a JSON object within the taskDefinition. This taskDefinition is then persisted in the database.

Example config/datasources/git-crawler.yaml:

type: git-crawler
repoUrl: https://github.com/your-org/your-repo.git
branch: main
depth: 1
# Note: localPath is handled dynamically as a temporary path by the crawler.

Example config/datasources/gdrive-crawler.yaml:

type: googledrive-crawler
folderId: your_google_drive_folder_id # The ID of the folder to crawl
authType: service_account
serviceAccountKeyPath: ./path/to/your/service-account-key.json # Path to your Google Service Account JSON key
# OR serviceAccountKey: "{\"type\": \"service_account\", ...}" # Direct JSON string
pageSize: 100

Example config/datasources/http-crawler.yaml:

type: http-crawler
startUrl: https://example.com/start-page
maxDepth: 2
recursiveCrawling: true
allowedDomains:
  - example.com
excludePaths:
  - /private
includePaths:
  - /public
requestTimeoutMs: 30000
userAgent: MyCustomCrawler/1.0

5.3. Output Format (IngestionData)
All crawlers transform their extracted data into a consistent IngestionData interface, making it easy for downstream processing.

export interface IngestionData {
    id: string; // Unique identifier for the data item (e.g., file ID, URL)
    content: string | Buffer | object; // The actual content (e.g., file content, HTML, JSON)
    metadata?: { // Optional metadata associated with the data
        [key: string]: any;
        filename?: string;
        relativePath?: string;
        changeType?: 'UPSERT' | 'DELETE' | 'UNKNOWN' | 'full_scan'; // Type of change
        // ... other source-specific metadata (commitSha, mimeType, etc.)
    };
    fetchedAt?: Date; // Timestamp when the data was fetched
    url?: string;     // Original URL of the data item
    statusCode?: number; // HTTP status code for web crawls
}

6. Crawler Types
6.1. Git Crawler (git-crawler.ts)
Purpose: Ingests code and metadata from Git repositories.

Configuration (GitCrawlerConfig): repoUrl (mandatory), branch, depth.

Authentication: Relies on the Git repository's accessibility (public, or private via SSH/HTTPS credentials handled by simple-git's environment setup, often managed by the Scheduler SDK providing a configured simple-git instance or environment variables). Webhook registration uses a GitHub Personal Access Token (PAT) via git-api-utils.ts.

Functionality:

Full Scan: Clones the entire repository to a temporary directory (os.tmpdir() + uuidv4()), reads all files, and then deletes the temporary directory.

Webhook Trigger: On a GitHub push webhook, it clones/pulls to a temporary directory, performs a git reset --hard to the specific commit, reads the added and modified files from the webhook's head_commit payload, and marks removed files. The temporary directory is then deleted.

Graceful Handling: Logs warnings for missing head_commit in webhooks or file read errors during full scans/webhooks, but does not crash.

6.2. Google Drive Crawler (gdrive-crawler.ts)
Purpose: Ingests files and changes from Google Drive folders.

Configuration (GoogleDriveCrawlerConfig): folderId (mandatory), authType (service_account), serviceAccountKeyPath or serviceAccountKey (JSON key content), pageSize.

Authentication: Uses Google Service Account (JWT) for secure server-to-server authentication.

Functionality:

Full Scan: Lists all files in the specified folderId, fetches their metadata and content. It supports resumable scans using nextPageToken.

Webhook Trigger (Delta Sync): When triggered by a Google Drive webhook, it utilizes a startPageToken (provided in the invocation payload) to call Google Drive's changes.list() API. This fetches only incremental changes.

It fetches content for added or modified files.

For removed or trashed files, it records DELETE changeType metadata without downloading content.

Token Management: Returns the newStartPageToken from changes.list to the Scheduler for persistence, ensuring the next sync starts from the correct point.

6.3. HTTP Crawler (http-crawler.ts)
Purpose: Crawls data from publicly accessible web pages and resources.

Configuration (HttpCrawlerConfig): startUrl (mandatory), maxDepth, recursiveCrawling, allowedDomains, excludePaths, includePaths, requestTimeoutMs, userAgent, headers.

Authentication: Typically handles public resources; for authenticated resources, credentials would be managed via headers in the config.

Functionality:

Uses axios for HTTP/HTTPS requests.

If recursiveCrawling is enabled, it parses HTML content using cheerio to extract links (a[href]) and follows them up to maxDepth, respecting domain and path filters.

Output: Provides fetched content and metadata including url, statusCode, contentType.

7. Webhook Processing & Utilities
These components facilitate the interaction between the Scheduler SDK and external services for webhook management.

processWebhookRequest.ts:

Role: A central utility invoked by the Scheduler SDK's webhook listener. It's responsible for the initial parsing and then full validation of incoming webhook payloads.

Validation: For GitHub, it verifies X-Hub-Signature headers against a stored secret. For Google Drive, it validates x-goog-channel-id against the stored externalWebhookId.

Payload Extraction: Extracts the relevant externalResourceId (e.g., Git repo URL, GDrive folder ID) and changeType (e.g., UPSERT, DELETE). It also gracefully handles GitHub "ping" events.

data-source-api-utils.ts:

Role: Acts as a generic dispatcher. The Scheduler SDK calls its registerWebhook() or deregisterWebhook() methods.

Dispatching: Based on the pluginType (e.g., git-crawler, googledrive-crawler), it routes the request to the appropriate service-specific utility (e.g., git-api-utils.ts, gdrive-api-utils.ts).

gdrive-api-utils.ts:

Role: Contains the specific API calls to Google Drive for managing webhook subscriptions.

Functionality: registerWebhook() (calls drive.files.watch(), returns Google's externalId (channel ID), channelResourceId, and startPageToken) and deregisterWebhook() (calls drive.channels.stop()). Uses Service Account (JWT) for authentication.

git-api-utils.ts:

Role: Contains the specific API calls to GitHub for managing webhook subscriptions.

Functionality: registerWebhook() (calls GitHub's hooks API, returns GitHub's webhook id as externalId) and deregisterWebhook() (calls GitHub's delete hook API). It uses a Personal Access Token (PAT) for authentication.

# Crawler SDK

A robust and extensible SDK for ingesting data from various external sources such as Git repositories, Google Drive, and HTTP endpoints.  
Designed to be stateless, the crawlers integrate seamlessly with an external **Scheduler SDK** for orchestration and task management.

---

## Table of Contents
1. [Introduction](#1-introduction)  
2. [Features](#2-features)  
3. [Architecture Overview](#3-architecture-overview)  
4. [Installation](#4-installation)  
    - [4.1. Prerequisites](#41-prerequisites)  
    - [4.2. Dependency Installation](#42-dependency-installation)  
5. [Usage](#5-usage)  
    - [5.1. General Concept](#51-general-concept)  
    - [5.2. Configuration](#52-configuration)  
    - [5.3. Output Format (IngestionData)](#53-output-format-ingestiondata)  
6. [Crawler Types](#6-crawler-types)  
    - [6.1. Git Crawler](#61-git-crawler-git-crawlerts)  
    - [6.2. Google Drive Crawler](#62-google-drive-crawler-gdrive-crawlerts)  
    - [6.3. HTTP Crawler](#63-http-crawler-http-crawlerts)  
7. [Webhook Processing & Utilities](#7-webhook-processing--utilities)  
8. [Testing](#8-testing)  
9. [License](#9-license)  

---

## **1. Introduction**

The Crawler SDK is a collection of specialized data source components built with TypeScript and Node.js. Its primary function is to efficiently extract and standardize data from diverse external systems. Designed with statelessness in mind, these crawlers are intended to be invoked by an external Scheduler SDK, which manages task orchestration, scheduling, and persistent state.

This SDK provides modular, testable, and extensible components for integrating various data sources into a larger data ingestion pipeline.

---

## **2. Features**
- **Multi-Source Data Ingestion:** Supports Git repositories, Google Drive, and HTTP/HTTPS endpoints.
- **Stateless Design:** Crawlers are stateless, making them highly scalable and resilient.
- **Full Data Scans:** Capability to perform complete initial data extractions.
- **Incremental Data Syncs (Delta Sync):** For supported sources (e.g., Google Drive), fetches only changes since the last successful ingestion using continuation tokens.
- **Webhook-Driven Triggers:** Processes incoming webhooks (e.g., GitHub push events, Google Drive change notifications) to trigger targeted ingestion.
- **Flexible Configuration:** Configurable via YAML files (for DataSource initialization) and dynamic payloads (for task execution).
- **Standardized Output:** Transforms extracted data into a consistent `IngestionData` format.
- **Temporary Local Storage:** Git operations utilize temporary local paths, ensuring no persistent local data is left behind.
- **Robust Error Handling & Logging:** Detailed logging and graceful error management during all stages.

---

## 3. Architecture Overview

The Crawler SDK operates as a set of stateless workers within a larger, loosely coupled architecture. It is designed to be invoked directly by an external Scheduler SDK.

### Key Principles:

- **Statelessness:** Each crawler instance created for a task execution is ephemeral. It does not maintain any long-term internal state. All necessary information (configuration, authentication, continuation tokens, webhook payloads) is passed to it at the point of invocation.

- **Single Responsibility:** Each crawler (`git-crawler.ts`, `gdrive-crawler.ts`, `http-crawler.ts`) focuses solely on data extraction from its specific source.

- **Separation of Concerns:** Webhook management (registration/deregistration) and payload processing are handled by dedicated utilities within the SDK, often invoked by the Scheduler SDK.

### Interaction Flow:

1. The Scheduler SDK (the orchestrator) determines when a task needs to run (e.g., cron schedule, webhook event, manual trigger).

2. The Scheduler SDK retrieves the full `taskDefinition` (including source configuration, authentication details, and latest continuation tokens) from its persistent database.

3. The Scheduler SDK directly instantiates a new instance of the relevant Crawler SDK component (e.g., `new GitCrawlerDataSource(...)`) and invokes its `execute()` method with a comprehensive `initialPayload`.

4. The invoked Crawler SDK instance performs the data extraction from the external source based only on the provided payload.

5. Upon completion, the Crawler SDK returns the `IngestionData` and a `GSStatus` (including any updated continuation tokens) back to the Scheduler SDK.

6. The Scheduler SDK then persists these updated tokens and the task status in its database for future runs.

This architecture ensures that the Crawler SDK remains lightweight, highly scalable, and resilient, as any instance can process any valid task request without needing prior history or complex state synchronization.

---

## 4. Installation
To use the Crawler SDK, you need to set up your Node.js environment and install the required dependencies.

### 4.1. Prerequisites
- Node.js (LTS version recommended)  
- npm (Node Package Manager) or pnpm  

### 4.2. Dependency Installation
Navigate to your project's root directory (where `package.json` is located) in your terminal and install the dependencies:

```bash
# Install runtime dependencies
npm install @godspeedsystems/core simple-git axios uuid googleapis google-auth-library cheerio

# Install development dependencies (for testing, TypeScript compilation)
npm install --save-dev jest ts-jest @types/jest ts-node @types/axios @types/cheerio
```
---
## 5. Usage


The Crawler SDK components are designed to be invoked by an external orchestrator (like the Scheduler SDK).

### 5.1. General Concept
Your Scheduler SDK will typically:

- Load a `taskDefinition` from its database.
- Based on `taskDefinition.source.pluginType`, dynamically determine which DataSource class to use (e.g., `git-crawler.ts`'s `DataSource`).
- Create an instance:  
  ```javascript
  const crawlerInstance = new DataSource({ config: taskDefinition.source.config });
  ```
 **Initialize the crawler's client:**
 ```typescript
await crawlerInstance.initClient();
```
**Execute the crawl:**
 ```typescript
const result = await crawlerInstance.execute(context, initialPayload);
```
**Process result.data.data (the IngestionData) and result.data.startPageToken / nextPageToken for persistence.**

### 5.2. Configuration

Each crawler type is configured via a `GitCrawlerConfig`, `GoogleDriveCrawlerConfig`, or `HttpCrawlerConfig` object. These configurations are typically defined in YAML files (e.g., `config/datasources/git-crawler.yaml`) and are used primarily for the initialization of the `DataSource` type when the system starts up. They serve as a reference and guide for developers to understand the available configuration parameters for each crawler.

Crucially, users do not have to explicitly create a separate YAML file for each new data source instance or task they register. Instead:

- When an **Administrator/User** defines a new ingestion task (e.g., via an API call to the Scheduler SDK),
- The specific configuration for that task's source is provided as a **JSON object** within the `taskDefinition`.
- This `taskDefinition` is then persisted in the database.

**Example: Git Crawler** (`config/datasources/git-crawler.yaml`)
```yaml
type: git-crawler
branch: main
depth: 1
```
**Example: Google Drive Crawler** (`config/datasources/gdrive-crawler.yaml`)
```yaml
type: googledrive-crawler
authType: service_account
serviceAccountKeyPath: ./path/to/service-account-key.json
pageSize: 100
```
**Example: HTTP Crawler** (`config/datasources/http-crawler.yaml`)
```yaml
type: http-crawler
maxDepth: 2
recursiveCrawling: true
allowedDomains:
  - example.com
```
### 5.3. Output Format (IngestionData)
- All crawlers transform their extracted data into a consistent `IngestionData` interface, making it easy for downstream processing.
```typescript
export interface IngestionData {
  id: string;
  content: string | Buffer | object;
  metadata?: {
    filename?: string;
    relativePath?: string;
    changeType?: 'UPSERT' | 'DELETE' | 'UNKNOWN' | 'full_scan';
  };
  fetchedAt?: Date;
  url?: string;
  statusCode?: number;
}
```

## 6. Crawler Types

### 6.1. Git Crawler (`git-crawler.ts`)

**Purpose:** Ingests code and metadata from Git repositories.

**Configuration (`GitCrawlerConfig`):** 
- `repoUrl` (mandatory)  
- `branch`  
- `depth`

**Authentication:**  
Relies on the Git repository's accessibility (public, or private via SSH/HTTPS credentials handled by `simple-git`'s environment setup, often managed by the Scheduler SDK providing a configured `simple-git` instance or environment variables). Webhook registration uses a GitHub Personal Access Token (PAT) via `git-api-utils.ts`.

**Functionality:**

- **Full Scan:**  
  Clones the entire repository to a temporary directory (`os.tmpdir() + uuidv4()`), reads all files, and then deletes the temporary directory.

- **Webhook Trigger:**  
  On a GitHub push webhook, it clones/pulls to a temporary directory, performs a `git reset --hard` to the specific commit, reads the added and modified files from the webhook's `head_commit` payload, and marks removed files. The temporary directory is then deleted.

- **Graceful Handling:**  
  Logs warnings for missing `head_commit` in webhooks or file read errors during full scans/webhooks, but does not crash.


### 6.2. Google Drive Crawler (`gdrive-crawler.ts`)

**Purpose:**  
Ingests files and changes from Google Drive folders.

**Configuration (`GoogleDriveCrawlerConfig`):**  
- `folderId` (mandatory)  
- `authType` (service_account)  
- `serviceAccountKeyPath` or `serviceAccountKey` (JSON key content)  
- `pageSize`  

**Authentication:**  
Uses Google Service Account (JWT) for secure server-to-server authentication.

**Functionality:**  

- **Full Scan:**  
  - Lists all files in the specified `folderId`  
  - Fetches their metadata and content  
  - Supports resumable scans using `nextPageToken`  

- **Webhook Trigger (Delta Sync):**  
  - Triggered by a Google Drive webhook  
  - Utilizes a `startPageToken` (provided in the invocation payload) to call Google Drive's `changes.list()` API  
  - Fetches only incremental changes  
  - Fetches content for added or modified files  
  - For removed or trashed files, records `DELETE` `changeType` metadata without downloading content  

- **Token Management:**  
  - Returns the `newStartPageToken` from `changes.list` to the Scheduler for persistence  
  - Ensures the next sync starts from the correct point


### 6.3. HTTP Crawler (`http-crawler.ts`)

**Purpose:**  
Crawls data from publicly accessible web pages and resources.

**Configuration (`HttpCrawlerConfig`):**  
- `startUrl` (mandatory)  
- `maxDepth`  
- `recursiveCrawling`  
- `allowedDomains`  
- `excludePaths`  
- `includePaths`  
- `requestTimeoutMs`  
- `userAgent`  
- `headers`  

**Authentication:**  
Typically handles public resources; for authenticated resources, credentials would be managed via headers in the config.

**Functionality:**  
- Uses `axios` for HTTP/HTTPS requests.  
- If `recursiveCrawling` is enabled, it parses HTML content using `cheerio` to extract links (`a[href]`) and follows them up to `maxDepth`, respecting domain and path filters.  

**Output:**  
Provides fetched content and metadata including `url`, `statusCode`, `contentType`.


## 7. Webhook Processing & Utilities

These components facilitate the interaction between the Scheduler SDK and external services for webhook management.

### `processWebhookRequest.ts`

- **Role:**  
  A central utility invoked by the Scheduler SDK's webhook listener. It's responsible for the initial parsing and then full validation of incoming webhook payloads.

- **Validation:**  
  - For GitHub, it verifies `X-Hub-Signature` headers against a stored secret.  
  - For Google Drive, it validates `x-goog-channel-id` against the stored `externalWebhookId`.

- **Payload Extraction:**  
  Extracts the relevant `externalResourceId` (e.g., Git repo URL, GDrive folder ID) and `changeType` (e.g., UPSERT, DELETE).  
  It also gracefully handles GitHub "ping" events.

### `data-source-api-utils.ts`

- **Role:**  
  Acts as a generic dispatcher. The Scheduler SDK calls its `registerWebhook()` or `deregisterWebhook()` methods.

- **Dispatching:**  
  Based on the `pluginType` (e.g., `git-crawler`, `googledrive-crawler`), it routes the request to the appropriate service-specific utility (e.g., `git-api-utils.ts`, `gdrive-api-utils.ts`).

### `gdrive-api-utils.ts`

- **Role:**  
  Contains the specific API calls to Google Drive for managing webhook subscriptions.

- **Functionality:**  
  - `registerWebhook()` – calls `drive.files.watch()`, returns Google's `externalId` (channel ID), `channelResourceId`, and `startPageToken`.  
  - `deregisterWebhook()` – calls `drive.channels.stop()`.  
  - Uses Service Account (JWT) for authentication.

### `git-api-utils.ts`

- **Role:**  
  Contains the specific API calls to GitHub for managing webhook subscriptions.

- **Functionality:**  
  - `registerWebhook()` – calls GitHub's hooks API, returns GitHub's webhook `id` as `externalId`.  
  - `deregisterWebhook()` – calls GitHub's delete hook API.  
  - Uses a Personal Access Token (PAT) for authentication.

## 8. Testing

The Crawler SDK emphasizes unit testing to ensure the reliability and correctness of individual components.

- **Framework:** Jest is used as the primary testing framework.  
- **TypeScript Integration:** `ts-jest` enables running tests written in TypeScript.  
- **Mocking:** External dependencies (like `simple-git`, `fs/promises`, `axios`, `googleapis`, `os`, `uuid`, and even internal logger) are extensively mocked using `jest.mock()` and `jest.spyOn()`. This ensures tests are isolated, deterministic, and fast, without making real network calls or file system operations.  
- **Test Structure:** Tests are organized in a `test/` directory, mirroring the `src/` structure (e.g., `test/datasources/types/git-crawler.test.ts`).  

**To run the tests:**

```bash
# Ensure all dependencies and devDependencies are installed
npm install

# Configure jest.config.js in your project root

# Run the test command
npm test
# or
npm run test

```
## 9. License

This project is licensed under the ISC License – see [LICENSE](LICENSE) for details.
