# Operational Details – Pipeline Execution and Incremental Processing

## 1. Overview

The project uses Azure Data Factory (ADF) to orchestrate manifest-driven ingestion of Olist CSV files into the Bronze layer in Azure Data Lake Storage Gen2.

The pipeline maintains processed-file state to identify files that have already been ingested and avoid processing them again during subsequent business-date runs.

Execution evidence is available for two business dates:

- Initial run: 2018-10-17
- Incremental run: 2018-10-18

## 2. Pipeline Execution Workflow

The ADF pipeline follows these steps:

1. **Read manifest:** Retrieves the list of source files and their corresponding destination paths.
2. **Read processed-file state:** Retrieves the list of files already processed.
3. **Identify pending files:** Filters the manifest to identify files not present in the processed-file list.
4. **Initialize tracking variable:** Loads the existing processed-file paths into a pipeline variable.
5. **Process files:** Iterates through the pending files sequentially.
6. **Copy to Bronze:** Reads CSV files from the configured HTTP source and writes them to the ADLS Gen2 Bronze location.
7. **Update processing state:** Adds a file to the tracking variable after its Copy activity succeeds.
8. **Persist processed-file state:** Updates the processed-files JSON using an ADF Web activity authenticated through Managed Identity.

## 3. Incremental Execution Evidence

The project demonstrates two execution scenarios.

### Initial execution – 17 October 2018

The initial run:

- Used business date `2018-10-17`.
- Read the manifest and existing processed-file state.
- Identified the files requiring ingestion.
- Landed nine source entities in the Bronze layer.
- Captured ingestion metadata for the landed records.

### Subsequent execution – 18 October 2018

The second run:

- Used business date `2018-10-18`.
- Used the existing processed-file state.
- Processed the next business-date batch.
- Extended the processed-file state with the newly processed files.
- Captured the new batch's Bronze metadata.

The second run represents incremental ingestion rather than another full ingestion of the original dataset.

## 4. Processing Metadata

The Copy activity adds the following metadata columns to the Bronze data:

| Column          | Purpose                                                         |
| --------------- | --------------------------------------------------------------- |
| `business_date` | Associates the record with the pipeline business-date parameter |
| `load_date`     | Records the business date used for the ingestion                |
| `ingested_at`   | Captures the UTC ingestion timestamp                            |
| `source_file`   | Identifies the source file                                      |

These fields support batch identification, traceability and downstream processing.

## 5. Sequential Processing and Success Dependencies

The ForEach activity is configured for sequential execution.

The processed-file tracking variable is updated only after the corresponding Copy activity succeeds.

The final processed-files JSON update depends on successful completion of the ForEach activity.

This establishes success dependencies between file ingestion and processing-state updates.

## 6. Retry Configuration

The exported ADF pipeline JSON shows a retry count of `0` for the Lookup, Copy and Web activities discussed in this implementation.

Therefore, automatic activity retries are not configured for these activities in the exported pipeline definition.

This should not be described as an automatic retry or self-healing mechanism.

## 7. Failure Behaviour and Operational Considerations

The current implementation uses activity dependencies to control the order of execution.

- The file-tracking append operation runs after a successful Copy activity.
- The final processed-files JSON update runs after successful completion of the ForEach activity.
- If the ForEach activity fails, its dependent final tracking update does not execute.

An operational consideration is that a file may have been copied successfully before a later iteration fails. Since the final tracking JSON update depends on the overall ForEach success, such a partial execution should be reviewed before rerunning the pipeline.

The current implementation should therefore be treated as a demonstration of sequential ingestion and processed-file tracking, not as a fully automated production recovery framework.

## 8. Monitoring and Evidence

The project includes visual evidence of:

- The initial ADF pipeline execution.
- The subsequent ADF pipeline execution.
- Bronze record counts and ingestion metadata.
- Processed-file state across the two business dates.
- Silver transformation and data-quality outcomes.
- Incremental MERGE scenarios for existing changed, existing unchanged and new records.
- SCD Type 2 and SCD Type 3 dimension behaviour.

The evidence demonstrates the recorded execution scenarios. It does not independently establish automated alerting, failure notifications, production SLA monitoring or deployment repeatability.

## 9. Operational Scope

The project demonstrates the following operational concepts:

- Manifest-driven file ingestion.
- Processed-file tracking.
- Sequential file processing.
- Success-dependent state updates.
- Business-date-based batch metadata.
- Incremental ingestion across multiple execution dates.
- Downstream incremental data processing.

These capabilities form the operational foundation of the project and provide evidence of how ingestion state is managed across successive pipeline executions.