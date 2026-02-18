# MyDocument — Assumptions, Steps, Learnings & Deployment

---

## Steps Followed

### Phase 1: Environment Setup
1. Spun up Spark cluster (1 master + 2 workers) and Jupyter via Docker Compose as per the guide.
2. Verified Spark Master UI at `http://localhost:8080` with 2 workers registered.
3. Opened Jupyter at `http://localhost:8888`.

### Phase 2: Data Ingestion & Cleaning
4. Read `nyc-jobs.csv` with PySpark.:
   - Initially saw incorrect column mapping and corrupt data in fields due to columns such as Job Description and Preferred Skills.
   - Resolved it using `multiLine=True`, `quote='"'`, `escape='"'`.
5. Fixed CSV header issue: last column name had trailing `\r` (CRLF line ending) causing it to appear nameless in schema.
   - used `chr(13)`/`chr(10)` to strip CR/LF.
6. Renamed all 28 columns to PySpark-friendly names.
7. Cast columns to proper types:
   - `num_of_positions` to int
   - `salary_range_from`, `salary_range_to` to double
   - `posting_date`, `post_until`, `posting_updated`, `process_date` to timestamp
8. Date casting required `regexp_replace()` to strip CR/LF from date string values.

### Phase 3: Data Exploration & Profiling
9.  Counted total rows and columns, classified columns by type (numeric, date, text/string).
10. Ran null/empty profiling for every column.
11. Computed descriptive statistics for numeric columns.
12. Checked date ranges (min/max) for all 4 date columns.
13. Identified categorical columns (low cardinality): `agency`, `posting_type`, `job_category`, `salary_frequency`, `full_time_part_time_indicator`, `level`.
14. Checked for duplicate job_ids.
15. Cleaned up extra, redundant code used during profiling.

### Phase 4: KPIs
16. **KPI 1:** Top 10 job categories by posting count — grouped by `job_category`, counted, sorted, limited to 10.
17. **KPI 2:** Salary distribution per category — computed min, max, mean of salary midpoint per category.
18. **KPI 3:** Degree–salary correlation — inferred education level from `minimum_qual_requirements` text; computed average salary per level and correlation.
19. **KPI 4:** Highest salary posting per agency — used `row_number()` window function partitioned by agency, ordered by salary desc.
20. **KPI 5:** Average salary per agency for last 2 years — filtered to postings within 24 months of max `posting_date`; grouped by agency.
21. **KPI 6:** Highest paid skills — parsed `preferred_skills` text into phrases; averaged salary per skill phrase.

### Phase 5: Data Processing Pipeline
22. Created reusable utility functions: `is_null_or_empty()`, `fill_null_or_empty()`, `null_count_report()`, `clean_and_cast_timestamp()`, `add_salary_midpoint()`, `add_salary_max()`, `normalize_category()`, `add_education_level()`.
23. Applied a few feature engineering techniques.
24. Removed low-value columns (optional).
25. Saved processed data as Parquet to `/dataset/processed/nyc_jobs_processed.parquet`.

### Phase 6: Optimization & Documentation & test cases
26. Consolidated all imports into a single cell at the top (no scattered imports).
27. Removed redundant code: duplicate `printSchema()` calls, repeated salary column definitions, unnecessary `from pyspark.sql.functions import *`.
28. Optimized KPI queries to select only required columns before aggregation.
29. Added clear markdown headers, assumptions in bold before each KPI, and code comments.
30. Added tests and mocks.

---

## Challenges

- **CSV parsing:** The CSV had Windows CRLF line endings. The last column header (`Process Date`) had a trailing `\r`, making it invisible in schema. Required `chr(13)`/`chr(10)` (not `\r`/`\n` literals) due to JSON/Python string escaping in Jupyter notebook format.
- **Date casting:** Spark's `trim()` removes only spaces, not `\r`/`\n`. Date string values had trailing `\r` from CRLF, causing casts to timestamp to return null. Solved with `regexp_replace()` using a pattern built at runtime from `chr(13)` + `chr(10)`.
- **Column name escaping:** Python escape sequences (`\r`, `\n`) in code are double-escaped in JSON (`\\r`, `\\n`). This caused `strip('\r\n')` to actually strip the letters `r` and `n` from column names (e.g. `process_date` → `pocess_date`). Solved by building the strip characters at runtime with `chr()`.
- **No degree column:** Had to infer education requirements from free-text `minimum_qual_requirements` using regex; not 100% precise.
- **No skills column:** Parsed `preferred_skills` free text by splitting on delimiters (bullets, numbering, semicolons, double spaces). Incrementally added regex as special characters appear in the results.
- **Using matplotlib.pyplot:** Spent time to create a decent visualization for KPI's.

---

## Considerations

- **Duplicate job IDs:** The same `job_id` can appear twice (Internal + External postings). KPIs count both unless stated otherwise.
- **Salary frequency:** Some salaries are Annual, some Hourly, some Daily. KPIs compare raw values without normalizing to a common frequency. For production use, converting all to annual would improve accuracy.
- **Data date range:** The data spans 2011–2019. "Last 2 years" is relative to the dataset's max date (Dec 2019), not today.

---

## Deployment / Trigger Approach

**If deploying this pipeline to production:**

1. **Convert notebook to a PySpark job:** Extract the cleaning, feature engineering, and KPI logic into a Python module (`nyc_jobs_pipeline.py`) that can be submitted with `spark-submit`.

2. **Orchestrate with Apache Airflow:**
   - DAG with tasks: `read_csv` → `clean_and_transform` → `compute_kpis` → `save_to_parquet` → `notify`.
   - Schedule: daily or weekly depending on how often new data arrives.
   - Use Airflow's `SparkSubmitOperator` or `BashOperator` to trigger the job.

3. **Infrastructure:**
   - Run on a managed Spark service / On prem setup's(e.g. AWS EMR, Databricks, GCP Dataproc, CDP).
   - Store raw CSV in S3/GCS/HDFS; write processed Parquet to a separate bucket/path.
   - Use a data catalog (e.g. AWS Glue, Hive metastore, UnityCatalog) for schema management.

4. **Monitoring:** Add logging within the PySpark job; Airflow provides task-level monitoring and alerting on failure.

---

## Learnings

- Spark CSV reader's `multiLine`, `quote`, and `escape` options are critical for real-world CSVs with embedded commas and quotes.
- Regex to parse text, with many special characters and delimiters.
- matplotlib.pyplot for building visualization.