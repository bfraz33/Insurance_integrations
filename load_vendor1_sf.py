#imports

import os
import snowflake.connector

# env variable credentials
SF_ACCOUNT   = os.getenv("SF_ACCOUNT")
SF_USER      = os.getenv("SF_USER")
SF_PASSWORD  = os.getenv("SF_PASSWORD")
SF_ROLE      = os.getenv("SF_ROLE")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_DATABASE  = os.getenv("SF_DATABASE")
SF_SCHEMA    = os.getenv("SF_SCHEMA")

STAGE_NAME   = os.getenv("SF_STAGE", "STG_VENDOR1")
TABLE_NAME   = os.getenv("SF_TABLE", "INSURANCE_DATA_RAW")

NDJSON_PATH  = os.getenv("NDJSON_PATH", r"C:\Insurance_integrations\vendor1_insurance.ndjson")


def win_path_to_snowflake_file_url(p: str):
    p = os.path.abspath(p)
    return "file://" + p.replace("\\", "/")

# printing sql statements in my terminal
def exec_sql(cur, sql: str):
    print("\nSQL>", sql)
    cur.execute(sql)

# checking file path
def main():
    if not os.path.exists(NDJSON_PATH):
        raise FileNotFoundError(f"Missing file: {NDJSON_PATH}")

    file_url = win_path_to_snowflake_file_url(NDJSON_PATH)

    print("Using NDJSON:", NDJSON_PATH)
    print("PUT file URL:", file_url)

    conn = snowflake.connector.connect(  # connecting to snowflake
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )

    try:
        cur = conn.cursor()

        exec_sql(cur, f"USE ROLE {SF_ROLE}")
        exec_sql(cur, f"USE WAREHOUSE {SF_WAREHOUSE}")
        exec_sql(cur, f"USE DATABASE {SF_DATABASE}")
        exec_sql(cur, f"USE SCHEMA {SF_SCHEMA}")

        # Staging & table
        exec_sql(cur, f"CREATE OR REPLACE STAGE {STAGE_NAME}")

        exec_sql(cur, f"""
            CREATE OR REPLACE TABLE {TABLE_NAME} (
              RAW_RECORD VARIANT,
              LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

        # PUT local file to internal stage
        put_sql = f"PUT '{file_url}' @{STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        exec_sql(cur, put_sql)

        #  COPY INTO table from stage
        copy_sql = f"""
            COPY INTO {TABLE_NAME} (RAW_RECORD)
            FROM (
              SELECT PARSE_JSON($1)
              FROM @{STAGE_NAME}
            )
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE'
        """
        exec_sql(cur, copy_sql)

        # Verifying loaded data
        exec_sql(cur, f"SELECT COUNT(*) FROM {TABLE_NAME}")
        count = cur.fetchone()[0]
        print("\n✅ Rows in table:", count)

        exec_sql(cur, f"SELECT RAW_RECORD FROM {TABLE_NAME} LIMIT 3")
        rows = cur.fetchall()
        print("\nSample rows:")
        for r in rows:
            print(r[0])

    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

if __name__ == "__main__":
    main()
