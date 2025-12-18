# Insurance ETL → Snowflake (NDJSON Loader)

This project loads insurance vendor data from a local **NDJSON** file into **Snowflake** using:

1. A Snowflake **internal stage** (`CREATE STAGE`)
2. `PUT` to upload the local file to the stage
3. `COPY INTO` to load each JSON line into a raw landing table (`VARIANT`)

The raw table pattern makes it easy to ingest first and transform later.

---

## What this does

- Converts a Windows file path into a Snowflake-friendly `file://` URL
- Creates/overwrites:
  - an internal stage (ex: `STG_VENDOR1`)
  - a raw table (ex: `INSURANCE_DATA_RAW`)
- Uploads your NDJSON file into the stage
- Loads each JSON record as a `VARIANT` into Snowflake
- Prints row count and sample records for verification

---

## Project layout (suggested)

