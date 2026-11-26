# Olist incremental batch 2 — test manifest

## Purpose
Controlled second-run test data built from the uploaded Olist source CSVs.
Existing Olist records are retained for update and unchanged scenarios.
One test-only inserted key per table is created by cloning a real Olist row's attributes.
Each file also contains an exact duplicate of its update row to exercise duplicate handling.

## Files
- `olist_customers_dataset_batch2.csv` — 4 rows
- `olist_products_dataset_batch2.csv` — 4 rows
- `olist_sellers_dataset_batch2.csv` — 4 rows

All CSV headers match the uploaded source CSV headers exactly. No ADF metadata columns are added to the CSVs; your ADF pipeline should add `load_date`, `ingested_at`, and `source_file` as currently configured.

## Test cases

### Customers
- UPDATE / SCD2 candidate: customer_id `06b8999e2fba1a1fbc88172c00ba8bc7`; customer_unique_id `861eff4711a542e4b93843c6dd7febb0`. Location fields changed to another real Olist location: `sao bernardo do campo, SP` (ZIP `09790`).
- UNCHANGED: customer_id `18955e83d337fd6b2def6b18a428ac77`.
- INSERT: test-only customer_id `3bd8af971828166afc2545609e3c0971`, customer_unique_id `09888a919d6b909cc0907877b502b3a2`; other attributes cloned from a real Olist row.
- DUPLICATE: exact duplicate of the UPDATE row; expected to be reduced to one source row by your Silver deduplication logic before Delta MERGE.

### Products
- UPDATE / SCD2 candidate: product_id `1e9e8ef04dbcff4541ed26657ea517e5`; category changed from `perfumaria` to `artes` (both category values occur in the uploaded Olist products file).
- UNCHANGED: product_id `3aa071139cb16b67ca9e5dea641aaa2f`.
- INSERT: test-only product_id `a0cc5b3711aea9c0c1fc1d3a73644dc3`; attributes cloned from a real Olist product.
- DUPLICATE: exact duplicate of the UPDATE row.

### Sellers
- UPDATE / SCD3 candidate: seller_id `3442f8959a84dea7ee197c632cb2df15`; location changed to another real Olist seller location: `mogi guacu, SP` (ZIP `13844`).
- UNCHANGED: seller_id `d1b65fc7debc3361ea86b5f14c68d2e2`.
- INSERT: test-only seller_id `8d270e91def2671686d726378d6bced1`; attributes cloned from a real Olist seller.
- DUPLICATE: exact duplicate of the UPDATE row.

## Important interpretation
- Olist is a static historical dataset. These are controlled simulated changes for demonstrating incremental processing and dimension change handling, not real new Olist transactions.
- Test-only inserted IDs are generated 32-character hexadecimal identifiers. They are not original Olist IDs.
- The update rows retain genuine Olist IDs, but their changed attributes are intentionally simulated.
- The files contain one exact duplicate row per table. This tests duplicate handling only if the existing Silver pipeline deduplicates on the relevant business key before MERGE. If it does not, a Delta MERGE may fail when multiple source rows match the same target row.
- Customer SCD2, product SCD2, and seller SCD3 will only be demonstrated if the existing Gold logic tracks the changed columns shown here and uses compatible business keys. We should confirm this against your actual notebook/code before running the Gold updates.
- Use a separate test copy/schema or a controlled test run. Do not overwrite your original source CSVs or production Gold tables.
- The pipeline's GitHub source path/file discovery rules must accept these `_batch2.csv` filenames. Confirm the configured source folder/pattern before the ADF run.
