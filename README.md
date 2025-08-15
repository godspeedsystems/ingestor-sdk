# Crawler SDK + Scheduler SDK (Simulated)

 ## Repository Structure (Branch: `Ingestor`)

This branch contains **two main folders**:

1. **`Ingestor/`**
   - Contains the **actual implementation code** for the Ingestor.
   - All core functionalities, modules, and logic reside here.

2. **`scheduler-sdk (simulated)/`**
   - Contains **simulated scheduler-related files**.
   - Each file has an **accompanying PDF** that:
     - Explains the purpose and current implementation of the file.
     - Documents **what is done** so far.
     - Lists **future scope** and pending tasks.

---
**Note:** The `scheduler-sdk` here is a **simulation** / lightweight (no DB persistence) setup to demonstrate the working of the Crawler SDK. 

# Ingestor

### INDEX

1. [Introduction](#1-introduction)  

2. [Features](#2-features)  

3. [Architecture Overview](#3-architecture-overview)  
   - [Key Principles](#key-principles)  
   - [Interaction Flow](#interaction-flow)  

4. [Project File Structure](#4-project-file-structure)  

5. [Setup & Usage Guide](#5-setup--usage-guide)  
   - [5.1. Files Involved in Setup](#51-files-involved-in-setup)  
   - [5.2. Installation](#52-installation)  
     - [5.2.1. Prerequisites](#521-prerequisites)  
     - [5.2.2. Dependency Installation](#522-dependency-installation)  
   - [5.3. Define Tasks (config/defaultyaml)](#53-define-tasks-configdefaultyaml)  
     - [5.3.1. Git Tasks (Cron + Webhook)](#531-git-tasks-cron--webhook)  
     - [5.3.2. Google Drive Tasks (Cron + Webhook)](#532-google-drive-tasks-cron--webhook)  
     - [5.3.3 HTTP Tasks (Cron)](#533-http-tasks-cron)  
   - [5.4. Main Setup File (src/final-testts)](#54-main-setup-file-srcfinal-testts)  
     - [5.4.1. Core Functionality](#541-core-functionality)  
   - [5.5. Cron Trigger Workflow](#55-cron-trigger-workflow)  
     - [5.5.1. Cron Event Definition](#551-cron-event-definition-configeventscron-testyaml)  
     - [5.5.2. Cron Event Handler](#552-cron-event-handler-srcfunctionstesttriggeringestionmanagercrontasksts)  
     - [5.5.3. How it Works Together](#553-how-the-cron-workflow-works-together)  
   - [5.6. Webhook Trigger Workflow](#56-webhook-trigger-workflow)  
     - [5.6.1. Webhook Event Definitions](#561-webhook-event-definitions-srceventsgdrive-eventyaml--srceventsgit_eventsyaml)  
     - [5.6.2. Webhook Event Handler](#562-webhook-event-handler-srcfunctionstesttriggeringestionmanagerwebhooktasksts)  
     - [5.6.3. How it Works Together](#563-how-the-webhook-workflow-works-together)  





# 1. Introduction

This project comprises two core components: a **Crawler SDK** and a **Scheduler SDK**.  

The **Crawler SDK** is a collection of specialized, stateless data source components built with TypeScript and Node.js. Its primary function is to efficiently extract and standardize data from diverse external systems like Git repositories, Google Drive, and HTTP endpoints.  

The **Scheduler SDK** (simulated in this project) acts as the central orchestrator, managing the lifecycle and execution of data ingestion tasks. It determines when and which crawler should run, providing all necessary context for each task.  

> **Important Note:** In this project's current state, the Scheduler SDK is simulated. This means it utilizes an **in-memory database** for task definitions and webhook registrations. There is no real database or persistent storage layer configured for the scheduler's state. All task and webhook data will be reset upon application restart. This setup is ideal for demonstration and testing purposes.

---

# 2. Features

1. **Multi-Source Data Ingestion** – Supports data extraction from various external sources, including Git repositories, Google Drive, and HTTP/HTTPS endpoints.  
2. **Stateless Design** – Crawler components are designed to be stateless, which makes them highly scalable and resilient as they don't retain memory between executions.  
3. **Full Data Scans** – Capability to perform comprehensive initial data extractions from a source.  
4. **Incremental Data Syncs (Delta Sync)** – For supported sources (e.g., Google Drive), the system can fetch only changes since the last successful ingestion by utilizing continuation tokens.  
5. **Webhook-Driven Triggers** – Processes incoming webhook events (e.g., GitHub push events, Google Drive change notifications) to trigger targeted and real-time data ingestion.  
6. **Flexible Configuration** – Allows for easy configuration of crawler behavior and task definitions via YAML files and dynamic payloads.  
7. **Standardized Output** – All extracted data is transformed into a consistent `IngestionData` format, simplifying downstream processing.  
8. **Temporary Local Storage** – Git operations utilize temporary local paths for cloning and pulling, ensuring no persistent local data is left behind after a task completes.  
9. **Robust Error Handling & Logging** – Provides detailed logging and graceful error management across all stages of the ingestion process.  

---

# 3. Architecture Overview

The **Crawler SDK** operates as a set of stateless workers within a larger, loosely coupled architecture. It's designed to be invoked directly by an external **Scheduler SDK**.  

## Key Principles

- **Statelessness** – Each crawler instance created for a task execution is ephemeral. It doesn't maintain any long-term internal state. All necessary information (configuration, authentication, continuation tokens, webhook payloads) is passed to it at the point of invocation.  
- **Single Responsibility** – Each crawler (`git-crawler.ts`, `gdrive-crawler.ts`, `http-crawler.ts`) focuses solely on data extraction from its specific source.  
- **Separation of Concerns** – Webhook management (registration/deregistration) and payload processing are handled by dedicated utilities within the SDK, often invoked by the Scheduler SDK.  

## Interaction Flow

1. The Scheduler SDK (the orchestrator) determines when a task needs to run (e.g., cron schedule, webhook event, manual trigger).  
2. The Scheduler SDK retrieves the full `taskDefinition` (including source configuration, authentication details, and latest continuation tokens) from its persistent database.  
3. The Scheduler SDK directly instantiates a new instance of the relevant Crawler SDK component (e.g., `new GitCrawlerDataSource(...)`) and invokes its `execute()` method with a comprehensive `initialPayload`.  
4. The invoked Crawler SDK instance performs the data extraction from the external source based only on the provided payload.  
5. Upon completion, the Crawler SDK returns the `IngestionData` and a `GSStatus` (including any updated continuation tokens) back to the Scheduler SDK.  
6. The Scheduler SDK then persists these updated tokens and the task status in its database for future runs.  

This architecture ensures that the Crawler SDK remains lightweight, highly scalable, and resilient, as any instance can process any valid task request without needing prior history or complex state synchronization.

# 4. Project File Structure

The project follows a modular structure, separating concerns into distinct directories for source code, configurations, and build outputs.

```plaintext
.
├── config/                # Configuration files for the application and tasks
│   └── default.yaml        # Main configuration file (logs, tasks loading)
│
├── dist/                  # Compiled JavaScript output (from TypeScript)
│   └── ... (compiled .js files and copied config/events/mappings)
│
├── node_modules/          # Project dependencies
│
├── src/                   # Source code directory
│   ├── datasources/
│   │   ├── types/
│   │   │   ├── axios.ts                  # (Internal/Helper) Axios types
│   │   │   ├── git-crawler.ts            # Git Crawler implementation
│   │   │   ├── googledrive-crawler.ts    # Google Drive Crawler implementation
│   │   │   ├── http-crawler.ts           # HTTP Crawler implementation
│   │   ├── git-crawler.yaml              # Example config for Git Crawler (reference)
│   │   ├── googledrive-crawler.yaml      # Example config for GDrive Crawler (reference)
│   │   ├── http-crawler.yaml             # Example config for HTTP Crawler (reference)
│   │
│   ├── events/                 # Godspeed event definitions
│   │   ├── cron-test.yaml        # Cron event definition
│   │   ├── gdrive-event.yaml     # GDrive webhook event definition
│   │   └── git_events.yaml       # GitHub webhook event definition
│   │
│   ├── eventsources/           # Godspeed event source types
│   │   └── types/
│   │       ├── cron.yaml           # Cron event source configuration
│   │       └── http.yaml           # HTTP event source configuration
│   │
│   ├── functions/
│   │   ├── Crawler/              # Webhook processing and external API utilities for Crawlers
│   │   │   ├── data-source-api-utils.ts       # Dispatches webhook API calls
│   │   │   ├── gdrive-api-utils.ts            # Google Drive webhook API calls
│   │   │   ├── git-api-utils.ts               # GitHub webhook API calls
│   │   │   └── processWebhookRequest.ts       # Central webhook payload processing
│   │   │
│   │   ├── my_bank_api/          # (Example/Placeholder for custom API functions)
│   │   │
│   │   ├── Scheduler/            # Core Scheduler SDK components
│   │   │   ├── FileSystemDestinationAdapter.ts      # Destination for saving crawled data locally
│   │   │   ├── GlobalIngestionLifecycleManager.ts   # Main Scheduler logic
│   │   │   ├── interfaces.ts                      # Shared interfaces (tasks, webhooks, data)
│   │   │   └── orchestrator.ts                    # Orchestrates single task execution
│   │   │
│   │   ├── TEST/                 # **Test/Simulation specific functions**
│   │   │   ├── final-test.ts                        # Main setup/simulation for Scheduler & Crawlers
│   │   │   ├── triggerIngestionManagerCronTasks.ts  # Handles cron-triggered task executions
│   │   │   └── triggerIngestionManagerWebhookTasks.ts # Handles webhook-triggered task executions
│   │   │
│   │   ├── Transformers/         # Data transformation functions
│   │   │   ├── code-metadata-extractor-transformer.ts
│   │   │   ├── gdrive-content-normalizer-transformer.ts
│   │   │   ├── generic-ingestion-preprocessor.ts
│   │   │   ├── html-to-plaintext-transformer.ts
│   │   │   ├── passthrough-transformer.ts
│   │   │   └── teams-message-cleaner-transformer.ts
│   │   │
│   │   └── validations/          # (Briefly: Request/Response Schema Validations)
│   │
│   └── index.ts                # Application entry point
│
├── .env                     # Environment variables (e.g., API keys, ngrok URLs)
├── package.json             # Project metadata and scripts
├── pnpm-lock.yaml           # Dependency lock file
├── tsconfig.json            # TypeScript compiler configuration

```


# 5. Setup & Usage Guide

This section provides a comprehensive, step-by-step guide to setting up and using your Crawler SDK and Scheduler SDK (Simulated). It covers dependency installation, environment configuration, task definition, and running tasks via various triggers, along with debugging tips.

## 5.1. Files Involved in Setup

The core files you'll interact with for setup and triggering are:

- **src/final-test.ts** – Main application setup file. Initializes the Scheduler SDK, registers default crawlers and destinations, loads tasks from configuration, and sets up event listeners.
- **src/functions/triggerIngestionManagerCronTasks.ts** – Acts as the Godspeed event handler for cron triggers. Invoked when a cron event is due.
- **src/functions/triggerIngestionManagerWebhookTasks.ts** – Serves as the Godspeed event handler for webhook triggers. Invoked when an external webhook notification is received.
- **config/events/cron-test.yaml** – Defines a cron event source, specifying the schedule for `triggerIngestionManagerCronTasks.ts`.
- **config/events/gdrive-event.yaml** – Defines the Google Drive webhook event source and endpoint for `triggerIngestionManagerWebhookTasks.ts`.
- **config/events/git_events.yaml** – Defines the GitHub webhook event source and endpoint for `triggerIngestionManagerWebhookTasks.ts`.

## 5.2. Installation

To use the Crawler SDK, you need to set up your Node.js environment and install the required dependencies.

### 5.2.1. Prerequisites

- Node.js (LTS version recommended)
- npm (Node Package Manager) or pnpm

### 5.2.2. Dependency Installation

Navigate to your project's root directory (where `package.json` is located) in your terminal and install the dependencies:

```bash
# Install runtime dependencies
npm install @godspeedsystems/core simple-git axios uuid googleapis google-auth-library cheerio
```

**Note on `@types` packages:**  
Some libraries (like `simple-git`, `googleapis`, and `google-auth-library`) bundle their TypeScript definitions directly, so explicit `@types/` packages for them might not be necessary. In some cases, installing them can cause conflicts (such as *E404 errors* during install).  

The command above includes only the `@types` packages that are typically required.

## 5.3. Define Tasks (`config/default.yaml`)

This section explains how to define ingestion tasks for the **Scheduler SDK**.  
Tasks are configured directly within your `config/default.yaml` file, serving as the central point for specifying their **source**, **destination**, and **trigger mechanisms**.

**Important:**  
Sensitive information such as API keys, service account paths, and webhook URLs **should NOT** be hardcoded directly in this YAML file.  
Instead, their values should be dynamically injected from environment variables (loaded via your `.env` file) by the `src/final-test.ts` file during application startup.  

This approach ensures:
- **Security** — credentials are never stored in source control.  
- **Flexibility** — you can easily switch configurations for different environments (development, staging, production).

### 5.3.1. Git Tasks (Cron + Webhook)

This subsection details the structure for defining tasks that interact with Git repositories, supporting both cron-based scheduling for full scans and webhook-based triggers for incremental updates.

---

#### Key Configuration Fields

- **id**: A unique identifier for this specific task (e.g., `webhook-git-clone`, `cron-git-clone`). This ID is used by the Scheduler SDK for tracking and management.
- **name**: A human-readable display name for the task. *(optional)*
- **enabled**: A boolean (`true`/`false`) to activate or deactivate the task. Only enabled tasks are considered for execution.
- **source**: Defines the data source for this task.
  - **pluginType**: Must be `git-crawler`. This tells the Scheduler SDK to use the Git Crawler component.
  - **config**: Contains specific settings for the Git Crawler:
    - **repoUrl**: The HTTPS URL of the Git repository to be crawled (e.g., `https://github.com/your-org/your-repo`). *(Mandatory)*
    - **branch**: The specific Git branch to track (e.g., `main`). Defaults to `main` if not specified.
    - **depth**: The depth of the Git clone (e.g., `1` for a shallow clone). Defaults to `1`.
- **destination**: Defines where the ingested data will be stored.
  - **pluginType**: Must be `file-system-destination`.
  - **config**: Contains settings for the destination, e.g., `outputPath: ./crawled_output/webhook-git`.
- **Optional Destination**:  
  If the `destination` block is omitted, the Scheduler SDK will not store the data locally. Instead, it will emit a `DATA_PROCESSED` event after the transformation stage.  
  This is useful for pipelines where data is consumed directly from events.
- **trigger**: Specifies how the task is initiated.
  - **type**: Can be `cron` or `webhook`.
  - **expression** (for cron type): A cron expression (e.g., `* * * * *` for every minute) defining the schedule.
  - **endpointId** (for webhook type): The local API endpoint that listens for incoming GitHub webhook notifications.  
    This must be `/webhook/github/` as per internal system logic.
  - **callbackurl** (for webhook type): The publicly accessible URL that you register with GitHub.  
    - If local, use a tool like ngrok (e.g., `https://your-ngrok-url.ngrok-free.app/api/v1/webhook/github/`).
    - If deployed, use your server’s public URL.
  - **credentials** (for webhook type): The GitHub Personal Access Token (PAT) used for authenticating webhook registration and Git operations.
- **currentStatus**: The initial status of the task when loaded, typically `SCHEDULED`.

---

#### Obtaining a GitHub Personal Access Token (PAT) 🔑

A PAT is required for your application to interact with GitHub's API (e.g., to register webhooks or clone private repositories).

1. **Log in to GitHub**: Go to [github.com](https://github.com) and log in.
2. **Navigate to Developer Settings**:  
   `Profile Picture` → **Settings** → **Developer settings** → **Personal access tokens** → **Tokens (classic)**.
3. **Generate New Token**:  
   Click **Generate new token** → **Generate new token (classic)**.
4. **Configure Token**:
   - **Name**: Give your token a descriptive name (e.g., `crawler-sdk-webhook`).
   - **Expiration**: Set an expiration date (e.g., `90 days`).
   - **Scopes**:  
     - Select **`admin:repo_hook`** (necessary for webhook creation/management).  
     - If private repo, also select **`repo`**.
5. **Generate Token**: Click **Generate token**.
6. **Copy Token**: Copy immediately — it will not be shown again. Store it securely (e.g., in your `.env` file).

---

#### Example `config/default.yaml` Snippet for Git Tasks

```yaml
# config/default.yaml

tasks:
  webhook-git-clone:
    id: webhook-git-clone
    name: Git Repository Webhook Clone
    enabled: true # Set to true to enable this task
    source:
      pluginType: git-crawler
      config:
        repoUrl: https://github.com/your-org/your-repo # Example repository URL
        branch: main
        depth: 1
    destination:
      pluginType: file-system-destination
      config: { outputPath: './crawled_output/webhook-git' }
    trigger:
      type: webhook
      # Values will be injected from process.env by final-test.ts
      credentials: "your_github_pat_placeholder" # From process.env.GITHUB_PAT
      endpointId: '/webhook/github/' # MANDATORY
      callbackurl: 'https://your-ngrok-url.ngrok-free.app/api/v1/webhook/github/' # From process.env.NGROK_GITHUB_WEBHOOK_URL or deployment URL
    currentStatus: SCHEDULED

  cron-git-clone:
    id: cron-git-clone
    name: Git Repository Cron Clone
    enabled: false # Set to true to enable this task
    source:
      pluginType: git-crawler
      config:
        repoUrl: https://github.com/your-org/your-repo
        branch: main
        depth: 1
    destination:
      pluginType: file-system-destination
      config: { outputPath: './crawled_output/cron-git' }
    trigger:
      type: cron
      expression: '* * * * *' # Every minute
    currentStatus: SCHEDULED
```

### 5.3.2 Google Drive Tasks (Cron + Webhook)

This subsection outlines the configuration for tasks designed to ingest data from **Google Drive**, supporting both scheduled full scans and real-time updates via webhooks.

---

### **Key Configuration Fields**

- **`id`**  
  A unique identifier for the task.  
  _Example:_  
  - `my-cron-google-drive-crawl-task`  
  - `my-webhook-google-drive-crawl-task`

- **`name`**  
  A human-readable display name for the task.

- **`enabled`**  
  Boolean (`true` / `false`) to activate or deactivate the task.

- **`source`**  
  Defines the data source for this task.  
  - **`pluginType`**: Must be `googledrive-crawler`.

- **`config`**  
  - **`folderId`**: The ID of the Google Drive folder to crawl (**mandatory**).  
  - **`authType`**: Must be `service_account`.  
  - **`serviceAccountKeyPath`**: Path to your Google Service Account JSON key file.  
  - **`userToImpersonateEmail`**: Email address of the user whose Google Drive account the service account will impersonate.  
  - **`pageSize`**: Number of files retrieved per page (default: `100`).

- **`destination`**  
  Defines where ingested data will be stored.  
  - **`pluginType`**: Must be `file-system-destination`.  
  - **`config`**:  
    ```yaml
    outputPath: ./crawled_output/cron-gdrive-data
    ```
  - **Optional Destination**: If omitted, the Scheduler SDK emits a `DATA_PROCESSED` event without local storage.

- **`trigger`**  
  Specifies how the task is initiated.  
  - **`type`**: `cron` or `webhook`.  
  - **For `cron`**:  
    - **`expression`**: Cron schedule. _Example:_ `* * * * *`  
  - **For `webhook`**:  
    - **`endpointId`**: Must be `/webhook/gdrive/`.  
    - **`callbackurl`**: Public HTTPS URL for Google Drive webhook registration.  
      - **Local**: Use [ngrok](https://ngrok.com/) —  
        Example: `https://your-ngrok-url.ngrok-free.app/api/v1/webhook/gdrive/`  
      - **Server**: Use deployment public URL.  
    - **`credentials`**: Includes `serviceAccountKeyPath` or key string for authentication.

- **`currentStatus`**  
  Initial status, typically `SCHEDULED`.

---

### **Obtaining Google Drive Credentials and Folder ID** 🔑

#### 1. Create a Google Service Account Key
1. Go to **Google Cloud Console**.
2. Select your project (or create one).
3. Navigate to: **IAM & Admin → Service Accounts**.
4. Click **+ CREATE SERVICE ACCOUNT** and follow prompts.
5. Assign a role (_e.g._, `Viewer`, `Editor`, or Drive-specific roles).
6. Click **+ CREATE KEY** → Choose **JSON** → **CREATE**.  
   A JSON key file will be downloaded — **store it securely**.

#### 2. Enable Google Drive API
1. In Google Cloud Console:  
   **APIs & Services → Enabled APIs and services → + ENABLE APIS AND SERVICES**
2. Search for **Google Drive API** → Click → **ENABLE**.

#### 3. Get the Service Account Email
- Found in your Service Account details.  
- _Example:_  
`my-service-account@your-project-id.iam.gserviceaccount.com`


#### 4. Get the Google Drive Folder ID
1. Open the target Google Drive folder in a browser.  
2. The URL format will be:  
`https://drive.google.com/drive/folders/<FOLDER_ID>`


#### 5. Share the Folder with Service Account
1. Right-click the folder → **Share**.  
2. Paste the Service Account email.  
3. Assign `Viewer` or `Editor` access.


####Example config/default.yaml Snippet
```yaml
# config/default.yaml

tasks:
  # ... (Git Tasks)

  my-cron-google-drive-crawl-task:
    id: my-cron-google-drive-crawl-task
    name: Google Drive Data Ingestion (Cron)
    enabled: true
    source:
      pluginType: googledrive-crawler
      config:
        serviceAccountKeyPath: "" 
        userToImpersonateEmail: ""
        folderId: ""
    destination:
      pluginType: file-system-destination
      config: { outputPath: './crawled_output/cron-gdrive-data' }
    trigger:
      type: cron
      expression: '* * * * *'
    currentStatus: SCHEDULED

  my-webhook-google-drive-crawl-task:
    id: my-webhook-google-drive-crawl-task
    name: Google Drive Data Ingestion (Webhook)
    enabled: false
    source:
      pluginType: googledrive-crawler
      config:
        serviceAccountKeyPath: "" 
        userToImpersonateEmail: ""
        folderId: ""
    destination:
      pluginType: file-system-destination
      config: { outputPath: './crawled_output/webhook-gdrive-data' }
    trigger:
      type: webhook
      endpointId: '/webhook/gdrive/'
      callbackurl: 'https://your-ngrok-url.ngrok-free.app/api/v1/webhook/gdrive/'
      credentials: { serviceAccountKeyPath: "" }
    currentStatus: SCHEDULED
```
### 5.3.3. HTTP Tasks (Cron)

This subsection details the structure for defining tasks that crawl publicly accessible web pages and resources via **HTTP/HTTPS**, typically triggered by a cron schedule.

---

#### **Key Configuration Fields**

- **id** – A unique identifier for the task  
  _Example_: `cron-http-crawl-daily`

- **name** – A human-readable display name for the task.

- **enabled** – A boolean (`true`/`false`) to activate or deactivate the task.

- **source** – Defines the data source for this task.  
  - **pluginType** – Must be `http-crawler`.  
  - **config** – Contains specific settings for the HTTP Crawler:  
    - **startUrl** – The initial URL from which the crawl will begin.  
      _Example_: `https://www.godspeed.systems/docs` _(mandatory)_  
    - **maxDepth** – Maximum depth for recursive crawling.  
      _Example_: `2` _(1 means only the startUrl is crawled)_  
    - **recursiveCrawling** – Boolean to enable/disable following links found on crawled pages.  
    - **sitemapDiscovery** – Boolean to enable/disable automatic sitemap discovery.  
    - **allowedDomains** – Optional array of domain names.  
      _Example_: `['example.com']`  
    - **excludePaths** – Optional array of URL path prefixes to ignore.  
      _Example_: `['/private']`  
    - **includePaths** – Optional array of URL path prefixes to follow exclusively.  
      _Example_: `['/public']`  
    - **requestTimeoutMs** – Timeout for HTTP requests in milliseconds.  
      _Example_: `30000`  
    - **userAgent** – User-Agent string for HTTP requests.  
      _Example_: `MyCustomCrawler/1.0`  
    - **headers** – Optional map of additional HTTP headers (e.g., for authentication).

- **destination** – Defines where the ingested data will be stored.  
  - **pluginType** – Must be `file-system-destination`.  
  - **config** – Contains settings for the destination.  
    _Example_: `outputPath: ./crawled_output/cron-http`

- **Optional Destination** – If omitted, the Scheduler SDK will emit a `DATA_PROCESSED` event after transformation without local storage.

- **trigger** – Specifies how the task is initiated.  
  - **type** – Must be `cron`.  
  - **expression** – Cron expression for scheduling.  
    _Example_: `* * * * *` _(every minute)_

- **currentStatus** – Initial status when loaded.  
  _Example_: `SCHEDULED`

---

#### **Example `config/default.yaml` Snippet for HTTP Tasks**

```yaml
# config/default.yaml

# ... (other global configs and Git/Google Drive tasks)

tasks:
  # ... (Git and Google Drive Tasks)

  cron-http-crawl-daily:
    id: cron-http-crawl-daily
    name: Daily Godspeed Website Crawl (Cron)
    enabled: true # Set to true to enable this task
    source:
      pluginType: http-crawler
      config:
        startUrl: https://www.godspeed.systems/docs
        maxDepth: 2
        recursiveCrawling: true
        sitemapDiscovery: false
        # requestTimeoutMs: 30000
        # userAgent: MyCustomCrawler/1.0
        # headers: { Authorization: "Bearer your_token" }
    destination:
      pluginType: file-system-destination
      config: { outputPath: './crawled_output/cron-http' }
    trigger:
      type: cron
      expression: '* * * * *' # Every minute
    currentStatus: SCHEDULED
```
## 5.4. Main Setup File (`src/final-test.ts`)

This file acts as the application's **primary entry point** for initializing the **Scheduler SDK** and configuring its initial state. It brings together the various components of your data ingestion pipeline and sets them up for operation.

---
### 5.4.1. **Core Functionality**

- **Scheduler Initialization**  
  Obtains the singleton instance of the `GlobalIngestionLifecycleManager` (the core of your Scheduler SDK).

- **Automated Source Registration**  
  Uses:
  ```typescript
  globalIngestionManager.sources(['git-crawler', 'googledrive-crawler', 'http-crawler']);
  ```
  to automatically register the default crawler types and their associated transformers.
This streamlines the setup process, avoiding repetitive manual registerSource calls.
- **Destination Registration**  
- It manually registers destination plugins, such as `FileSystemDestinationAdapter`, which defines where the processed data will be stored locally.

- **Task Loading and Scheduling**  
- It loads all defined ingestion tasks from the `tasks` section of your application's configuration (which comes from `config/default.yaml`).
- For each loaded `taskDefinition`, it dynamically injects sensitive data (like API keys, service account paths, and webhook callback URLs) directly from `process.env` (your environment variables, loaded from `.env`).  
  This is crucial for security, preventing sensitive information from being hardcoded in configuration files.
- After injecting the dynamic values, it calls `globalIngestionManager.scheduleTask()` for each task, adding it to the Scheduler's management.
  
 - **Event Listeners**
- It sets up event listeners on the `globalIngestionManager`'s internal event bus.  
- These listeners capture and log important events such as:
  - `TASK_COMPLETED`
  - `TASK_FAILED`
  - `DATA_TRANSFORMED`  
- These are invaluable for debugging and monitoring the ingestion pipeline's activity.

- **Manager Startup**
- Finally, it calls:
  ```javascript
  globalIngestionManager.init();
  globalIngestionManager.start();
  ```
  to initialize the Scheduler's database (in-memory in this simulation) and begin its operation, making it ready to process triggers

 ## **5.5. Cron Trigger Workflow**

This section explains how tasks configured with a cron schedule are automatically executed by the Scheduler SDK. It details the role of the cron event definition and its corresponding handler function.

---

### **5.5.1. Cron Event Definition (`config/events/cron-test.yaml`)**

This YAML file defines a cron event source within the Godspeed framework.  
It tells the Godspeed server to periodically emit an event based on a specified cron expression, which then triggers a designated function in your application.
```yaml
# C:\Users\SOHAM\Desktop\Crawler-sdk\crawler\src\events\cron-test.yaml

cron.* * * * *.Asia/Kolkata: # The cron expression and timezone define the schedule
  fn: TEST.triggerIngestionManagerCronTasks # This specifies the function to call when the cron event is due
```
**cron.* * * * *.Asia/Kolkata:** This is the event source definition.

- **cron:** Indicates it's a cron event.  
- **\* \* \* \* \*:** This is the standard cron expression, meaning "every minute." You can customize this to run tasks hourly, daily, etc.  
- **Asia/Kolkata:** Specifies the timezone for the cron schedule.  
- **fn: TEST.triggerIngestionManagerCronTasks:** This is the handler function that Godspeed will invoke when the cron event is due. It points to the `triggerIngestionManagerCronTasks` function located within the `TEST` directory under `src/functions/`.

### 5.5.2. Cron Event Handler (`src/functions/TEST/triggerIngestionManagerCronTasks.ts`)

This TypeScript file contains the **Godspeed event handler** that is executed whenever a cron event, as defined in `cron-test.yaml`, is triggered.

- **Primary Role:**  
  To delegate the task of identifying and executing due cron tasks to the `GlobalIngestionLifecycleManager`.

 ```typescript
// C:\Users\SOHAM\Desktop\Crawler-sdk\crawler\src\functions\TEST\triggerIngestionManagerCronTasks.ts

import { GSContext, logger } from '@godspeedsystems/core';
import { globalIngestionManager } from './final-test' // Imports the singleton instance of the GlobalIngestionLifecycleManager

/**
 * Godspeed event handler for cron triggers.
 * This function is expected to be called by a Godspeed cron event source
 * (e.g., configured in events/cron_events.yaml).
 * It instructs the GlobalIngestionLifecycleManager to check and trigger
 * any scheduled cron tasks that are due.
 *
 * @param ctx The Godspeed context object, containing event details.
 * @returns A GSStatus indicating the result of triggering cron tasks.
 */
export default async function (ctx: GSContext) {
    logger.info("--- Received cron trigger. Initiating comprehensive manager method check. ---");
    logger.info("Attempting to trigger all enabled cron tasks via GlobalIngestionLifecycleManager.");
    
    try {
        // Calls the GlobalIngestionLifecycleManager's method to find and trigger due cron tasks.
        // The manager handles all the internal logic like checking schedules, fetching tasks from DB,
        // and invoking the crawlers.
        const status = await globalIngestionManager.triggerAllEnabledCronTasks(ctx);

        // Returns the status received from the manager's operation.
        return status;
    } catch (error: any) {
        // Catches any unexpected errors during the triggering process and logs them.
        // Returns a failed GSStatus to indicate an issue.
        logger.error(`Error in triggerIngestionManagerCronTasks: ${error.message}`, { error });
        return { success: false, message: `Failed to trigger cron tasks: ${error.message}` };
    }
}
```
### 5.5.3. How the Cron Workflow Works Together

- **Cron Event Source (Godspeed Framework)**  
  The Godspeed framework, configured by `config/events/cron-test.yaml`, acts as a timer. At each specified interval (e.g., every minute), it emits a `cron.* * * * *.Asia/Kolkata` event.

- **Event Handler Invocation**  
  The Godspeed framework automatically routes this event to the `triggerIngestionManagerCronTasks.ts` function, as specified by the `fn` property in the YAML.

- **Delegation to Scheduler SDK**  
  The `triggerIngestionManagerCronTasks.ts` function's sole purpose is to call:  
  ```typescript
  globalIngestionManager.triggerAllEnabledCronTasks(ctx)
- **Scheduler SDK's Orchestration:**

The **GlobalIngestionLifecycleManager** then takes over:

- It queries its in-memory database to **List Enabled Cron Tasks**.
- It uses **cron-parser** to **Identify Due Tasks** by comparing the task's expression with the current time.
- For each due task, it retrieves:
  - The full **Task Definition**
  - Any necessary continuation tokens (like `startPageToken` for Google Drive delta syncs) from its database.
- It then invokes a new, dedicated instance of the appropriate **Crawler SDK** component  
  (e.g., `git-crawler.ts`, `gdrive-crawler.ts`, `http-crawler.ts`) with a **Crawler Invocation Payload**.
- After the crawler executes, the **GlobalIngestionLifecycleManager**:
  - Updates the task's status
  - Persists any new continuation tokens in its database.
- Finally, it reports the overall **Execution Status & Data** to:
  - The console
  - Other configured outputs.

This workflow ensures that tasks are executed reliably at scheduled intervals,  
with the **Scheduler SDK** managing the state and coordination.

## 5.6. Webhook Trigger Workflow

This section details how tasks are executed in response to external webhook notifications from services like GitHub or Google Drive. It emphasizes the multi-stage validation steps, the retrieval of resource-specific secrets from the database, and the subsequent invocation of the appropriate Crawler SDK component.

### 5.6.1. Webhook Event Definitions (`src/events/gdrive-event.yaml` & `src/events/git_events.yaml`)

These YAML files define webhook event sources within the Godspeed framework. They instruct the Godspeed server to listen for incoming HTTP POST requests at specific API endpoints. When a request arrives at one of these endpoints, Godspeed automatically invokes the designated handler function.

**Example** `src/events/git_events.yaml`
```yaml
# C:\Users\SOHAM\Desktop\Crawler-sdk\crawler\src\events\git_events.yaml

http.post./webhook/github/: # Defines an HTTP POST endpoint at /webhook/github/
  id : github-push-event # Unique identifier for this event
  fn: TEST.triggerIngestionManagerWebhookTasks # The function to call when a request is received
  summary: GitHub Push Event Webhook
  description: Receives GitHub push event payloads to trigger Git crawling tasks.
  authn: false # Authentication for this endpoint is handled by internal webhook validation
  body:
    content:
      application/json:
        schema: {} # Schema can be defined here if needed for validation
  responses:
    200:
      description: Webhook received and processed successfully.
      content:
        application/json:
          schema:
            type: object
            properties:
              success: { type: boolean }
              message: { type: string }
```
**Example** `src/events/gdrive-event.yaml`
```yaml
# C:\Users\SOHAM\Desktop\Crawler-sdk\crawler\src\events\gdrive-event.yaml

http.post./webhook/gdrive/: # Defines an HTTP POST endpoint at /webhook/gdrive/
  id : gdrive-push-event # Unique identifier for this event
  fn: TEST.triggerIngestionManagerWebhookTasks # The function to call when a request is received
  summary: Googledrive Push Event Webhook
  description: Receives Googledrive push event payloads to trigger gdrive crawling tasks.
  authn: false # Authentication for this endpoint is handled by internal webhook validation
  body:
    content:
      application/json:
        schema: {} # Schema can be defined here if needed for validation
  responses:
    200:
      description: Webhook received and processed successfully.
      content:
        application/json:
          schema:
            type: object
            properties:
              success: { type: boolean }
              message: { type: string }
```
**http.post./webhook/github/** / **http.post./webhook/gdrive/**  
- These define the specific API endpoints that your Godspeed application will expose.  
- External services like GitHub or Google Drive will send their webhook notifications to these URLs.  

**id**  
- A unique identifier for the event definition.  

**fn: TEST.triggerIngestionManagerWebhookTasks**  
- This specifies the handler function that Godspeed will execute when a notification is received at the endpoint.  
- It points to the `triggerIngestionManagerWebhookTasks` function located within the `TEST` directory under `src/functions/`.  

**authn: false**  
- This indicates that Godspeed's built-in authentication for this HTTP endpoint is disabled.  
- This is because webhook authentication (signature/secret validation) is handled internally by your Scheduler SDK's logic (`processWebhookRequest.ts`) for greater control and flexibility.  

### 5.6.2. Webhook Event Handler (`src/functions/TEST/triggerIngestionManagerWebhookTasks.ts`)

This TypeScript file contains the **Godspeed event handler** that is executed whenever a webhook notification, as defined in the event YAMLs, is received.  

Its crucial role is to:  

- Manage the **multi-stage validation**.  
- Dispatch the **task execution** to the `GlobalIngestionLifecycleManager`.  
 ```typescript
C:\Users\SOHAM\Desktop\Crawler-sdk\crawler\src\functions\TEST\triggerIngestionManagerWebhookTasks.ts

import { GSContext, GSStatus, logger } from "@godspeedsystems/core";
import { globalIngestionManager } from './final-test' // Imports the singleton instance of the GlobalIngestionLifecycleManager
import { ProcessedWebhookResult } from '../ingestion/interfaces'; // Required for type hinting

/**
 * Godspeed event handler for webhook triggers.
 * This function is automatically invoked by the Godspeed framework when a webhook
 * notification is received at a configured endpoint (e.g., /webhook/github/).
 * It orchestrates the validation and processing of the webhook, then triggers
 * the relevant ingestion tasks via the GlobalIngestionLifecycleManager.
 *
 * @param ctx The Godspeed context object, containing raw request inputs.
 * @returns A GSStatus indicating the result of webhook processing and task triggering.
 */
export default async function (ctx: GSContext): Promise<GSStatus> {
    logger.info("------------Received webhook event.-----------------------"); // Log webhook reception

    // Extract raw webhook payload, headers, and endpoint ID from the Godspeed context.
    const webhookPayload = (ctx.inputs as any).data.body;
    const requestHeaders = (ctx.inputs as any).data.headers;
    const endpointId = (ctx.inputs as any).type; // Represents the endpoint ID (e.g., '/webhook/github/')

    try {
        // Delegate the webhook processing to the GlobalIngestionLifecycleManager.
        // This manager handles validation, task identification, and crawler invocation.
        const result = await globalIngestionManager.triggerWebhookTask(ctx, endpointId, webhookPayload, requestHeaders);

        // Check if the webhook processing failed or returned an invalid result.
        if (!result.success) {
            logger.warn(`Failed to process webhook: ${result.message || 'Unknown error'}`, { result });
            return new GSStatus(false, 400, `Webhook validation failed: ${result.message || 'Unknown error'}`);
        }

        // --- Optional: Code for delayed task deletion as the scheduler is not using db so use these method for deregistering webhook(currently commented out) ---
        // This section demonstrates how tasks could be programmatically deleted after a delay.
        // It's commented out, so it does not execute during normal operation.
        // logger.warn("delete initiated.....")
        // function sleep(ms: number): Promise<void> {
        //     return new Promise(resolve => setTimeout(resolve, ms));
        // }
        // async function deleteWithDelay() {
        //     await sleep(30000); // wait for 30 seconds
        //     await globalIngestionManager.deleteTask('my-webhook-google-drive-crawl-task');
        //     const response = await globalIngestionManager.deleteTask('webhook-git-clone');
        //     console.log("Delete response:", response);
        // }
        // deleteWithDelay();
        // --- End of optional deletion code ---

        // Return a successful status indicating the webhook event was processed.
        return new GSStatus(true, 200, "Webhook event processed and ingestion triggered.", result);

    } catch (error: any) {
        // Catch any unexpected errors during webhook processing and return a failure status.
        logger.error(`Error while processing webhook: ${error.message}`, { error });
        return new GSStatus(false, 500, `Internal server error: ${error.message}`);
    }
}
```
### 5.6.3. How the Webhook Workflow Works Together

- **External Webhook Notification**  
  An External Data Source (e.g., GitHub, Google Drive) sends an HTTP POST Webhook Notification to the publicly accessible `callbackurl` (provided by ngrok for local, or your deployment server).

- **Godspeed Webhook Listener**  
  The Godspeed framework, configured by `config/events/git_events.yaml` or `gdrive-event.yaml`, receives this incoming HTTP POST request at the specified `endpointId` (e.g., `/webhook/github/`).

- **Event Handler Invocation**  
  Godspeed automatically invokes the `triggerIngestionManagerWebhookTasks.ts` function, passing the raw `webhookPayload` and `requestHeaders` within the `ctx` object.

- **Delegation to Scheduler SDK**  
  The `triggerIngestionManagerWebhookTasks.ts` function delegates the entire processing to  
  ```ts
  globalIngestionManager.triggerWebhookTask(ctx, endpointId, webhookPayload, requestHeaders);
  ```
- **Scheduler SDK's Multi-Stage Processing**  

The **GlobalIngestionLifecycleManager** orchestrates the following steps:

  **1. Preliminary Processing**  
  - Calls `processWebhookRequest.ts` (a **Crawler SDK utility**) to **Preliminary Process Payload**.  
  - Extracts the `externalResourceId` (e.g., GitHub repo URL, Google Drive folder ID) from the raw payload.

  **2. Database Lookup**  
  - Uses this `externalResourceId` to look up the **Webhook Registration** record in its **Webhook Registrations Data Store**.  
  - This record contains:  
    - `secret` (for validation)  
    - `externalWebhookId` (Google's Channel ID)  
    - `channelResourceId`  
    - List of `registeredTasks` associated with this webhook.

  **3. Full Validation**  
  - Passes the raw `webhookPayload`, `requestHeaders`, and the retrieved `secret` / `externalWebhookId` back to `processWebhookRequest.ts` for **Full Validation**.  
  - Examples include:  
    - Checking **GitHub's** `X-Hub-Signature`  
    - Checking **Google Drive's** `x-goog-channel-id`.

  **4. Task Identification**  
  - If the webhook is `isValid`, it identifies all **enabled tasks** from the `registeredTasks` array in the **Webhook Registration** record that need to be triggered.

  **5. Crawler Invocation**  
  - For each identified task:  
    - Retrieves its full **Task Definition** from the **Ingestion Tasks Data Store**.  
    - Prepares a **Crawler Invocation Payload** including:  
      - Parsed `webhookPayload`  
      - `externalResourceId`  
      - `changeType`  
      - Latest `startPageToken` / `nextPageToken` from the database  
    - Invokes the relevant **Crawler SDK Component** (e.g., `git-crawler.ts`, `gdrive-crawler.ts`'s `execute()` method).

  **6. Status & Token Persistence**  
  - After the crawler executes, the **GlobalIngestionLifecycleManager** updates the task's status.  
  - Persists any new continuation tokens back into the database.

---

- **Report Execution Status**  
  - The `triggerIngestionManagerWebhookTasks.ts` function returns the **GSStatus** from the **GlobalIngestionLifecycleManager** back to **Godspeed**.  
  - **Godspeed** sends the appropriate HTTP response (e.g., `200 OK`) back to the external service that sent the webhook.










