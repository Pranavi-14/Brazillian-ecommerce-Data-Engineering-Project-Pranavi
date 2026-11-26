# Azure Data Factory – Pipeline Design

## 1. Overview

The Azure Data Factory pipeline implements incremental file ingestion from the GitHub-hosted source datasets into Azure Data Lake Storage Gen2.

The pipeline uses a manifest-driven approach and a processed-file tracking mechanism to identify files that have not been ingested previously.

The exported pipeline configuration is available at:

`ADF/pipelines/pl_github_incremental_to_bronze.json`

**Pipeline name:** `pl_github_incremental_to_bronze`

## 2. Pipeline Architecture

The pipeline follows this execution sequence:

```
lk_read_manifest
       |
       v
lk_read_processed_files
       |
       v
fl_unprocessed_files
       |
       v
sv_initialize_processed_files
       |
       v
fe_process_files
       |
       +---- cp_github_to_bronze
       |             |
       |             v
       |     av_add_successful_file
       |
       v
web_update_processed_files
```

The ForEach activity processes the filtered files sequentially.

## 3. Pipeline Activities

| Activity                        | Type           | Responsibility                                          |
| ------------------------------- | -------------- | ------------------------------------------------------- |
| `lk_read_manifest`              | Lookup         | Reads the source file manifest                          |
| `lk_read_processed_files`       | Lookup         | Reads the previously processed file list                |
| `fl_unprocessed_files`          | Filter         | Identifies files not present in the processed-file list |
| `sv_initialize_processed_files` | SetVariable    | Initializes the processed-file tracking variable        |
| `fe_process_files`              | ForEach        | Iterates through the unprocessed files                  |
| `cp_github_to_bronze`           | Copy           | Copies source CSV files into Bronze storage             |
| `av_add_successful_file`        | AppendVariable | Adds successfully copied files to the tracking variable |
| `web_update_processed_files`    | WebActivity    | Updates the processed-file tracking JSON                |

## 4. Incremental Ingestion Logic

The pipeline uses two JSON configuration files:

- **Manifest:** Defines the source files and their corresponding destination information.
- **Processed-files tracking file:** Maintains the list of files already processed.

The execution logic is as follows:

1. Read the source manifest using `lk_read_manifest`.
2. Read the existing processed-file list using `lk_read_processed_files`.
3. Compare the manifest entries against the existing processed-file list.
4. Use `fl_unprocessed_files` to select only files that are not already recorded as processed.
5. Initialise the tracking variable with the existing processed-file list.
6. Process the filtered files using the ForEach activity.
7. Append a file name to the tracking variable only after its Copy activity succeeds.
8. Update the processed-file tracking JSON after the ForEach activity completes successfully.

The filter uses the following ADF expression:

```
@not(contains(activity('lk_read_processed_files').output.value[0].processed_files, item().p_sink_file))
```

This checks whether the destination file name is already present in the processed-file list.

## 5. Source-to-Bronze Copy Operation

The Copy activity is named:

`cp_github_to_bronze`

### Source Configuration

- Source format: Delimited text (CSV)
- Source access: HTTP
- Request method: GET
- Source dataset: `ds_github_dynamic_csv`
- Source file path: Dynamically supplied through the manifest.

### Destination Configuration

- Sink format: Delimited text
- Destination dataset: `ds_bronze_dynamic_csv`
- Storage: Azure Data Lake Storage Gen2
- Copy behaviour: Preserve hierarchy
- Output file extension: `.txt`

The destination folder and file name are dynamically supplied using manifest values.

### Additional Metadata Columns

The Copy activity adds the following columns during ingestion:

| Column          | Value                      | Purpose                                           |
| --------------- | -------------------------- | ------------------------------------------------- |
| `business_date` | `p_business_date`          | Associates the ingested data with a business date |
| `load_date`     | `p_business_date`          | Records the configured load date                  |
| `ingested_at`   | `utcNow()`                 | Captures the ingestion timestamp                  |
| `source_file`   | Extracted from `p_rel_url` | Identifies the source file                        |

These columns provide useful metadata for downstream processing and traceability.

## 6. Sequential File Processing

The ForEach activity is named:

`fe_process_files`

Its configuration uses:

```
"isSequential": true
```

This means the files are processed one at a time rather than concurrently.

Within each iteration:

1. `cp_github_to_bronze` copies the current source file.
2. `av_add_successful_file` appends the destination file name to `v_processed_files` only when the Copy activity succeeds.

This prevents an unsuccessful Copy activity from being added to the processed-file tracking list.

## 7. Processed-File Tracking Update

The final activity is:

`web_update_processed_files`

It executes only after the ForEach activity succeeds.

The activity uses:

- HTTP method: `PUT`
- Authentication: Managed Identity
- Content type: `application/json`
- Destination: Azure Blob Storage processed-file tracking JSON

The request body is constructed using the updated `v_processed_files` variable.

This updates the tracking file so that subsequent pipeline executions can identify files already processed.

## 8. Execution and Failure Behaviour

The exported pipeline configuration specifies the following retry settings:

| Activity                     | Retry Count | Timeout  |
| ---------------------------- | ----------- | -------- |
| `lk_read_manifest`           | 0           | 12 hours |
| `lk_read_processed_files`    | 0           | 12 hours |
| `cp_github_to_bronze`        | 0           | 12 hours |
| `web_update_processed_files` | 0           | 12 hours |

The other listed activities do not define their own retry policies in the exported JSON.

### Success-dependent execution

- The Copy activity must succeed before its file name is appended to the tracking variable.
- The processed-file tracking update depends on successful completion of the ForEach activity.
- If the ForEach activity does not succeed, the final tracking update is not executed through its configured success dependency.

### Retry configuration

The explicitly configured activities have a retry count of zero. Therefore, automatic activity retries are not configured in this pipeline export.

## 9. Important Implementation Considerations

- The processed-file list is used to avoid selecting previously processed destination file names.
- File processing is sequential.
- Successfully copied files are added to the tracking variable.
- The tracking JSON is updated after successful completion of the file-processing loop.
- The pipeline uses Managed Identity for the final tracking-file update.

The implementation should be reviewed carefully when adapting it to another Azure environment, particularly the storage endpoint, permissions, dataset parameters and manifest structure.

## 10. Related Repository Files

| File / Directory                                     | Description                                   |
| ---------------------------------------------------- | --------------------------------------------- |
| `ADF/pipelines/pl_github_incremental_to_bronze.json` | Main ingestion pipeline                       |
| `ADF/datasets/ds_config_manifest.json`               | Manifest dataset                              |
| `ADF/datasets/ds_processed_files.json`               | Processed-file tracking dataset               |
| `ADF/datasets/ds_github_dynamic_csv.json`            | Dynamic HTTP source dataset                   |
| `ADF/datasets/ds_bronze_dynamic_csv.json`            | Dynamic Bronze destination dataset            |
| `ADF/linkedServices/`                                | ADF connection configurations                 |
| `Data/`                                              | Sample source data organised by business date |

---

**Note:** This document describes the exported ADF configuration. Actual execution outcomes should be verified using the pipeline run history and supporting evidence in the `Evidence/` directory.