import os
import duckdb

LANDING_DIR = r"C:\tmp\analytics\landing"
DB_PATH = r"C:\tmp\analytics\warehouse.db"

def run_ingestion_pipeline():
    con = duckdb.connect(DB_PATH)
    
    json_files_pattern = os.path.join(LANDING_DIR, "*.json")
    
    if not os.path.exists(LANDING_DIR) or not any(f.endswith('.json') for f in os.listdir(LANDING_DIR)):
        print("No new log files found in landing zone to ingest.")
        return

    print("Staging, flattening, and deduplicating raw tracking payloads...")

    # SQL Pipeline Logic:
    # 1. Reads raw JSON files lines on-the-fly using DuckDB's native JSON reader
    # 2. Dot-notates into nested structures (`user_context.user_id`)
    # 3. Uses a ROW_NUMBER() window function to drop duplicate event_ids
    pipeline_query = f"""
        CREATE OR REPLACE TEMP TABLE staged_events AS
        WITH raw_extracted AS (
            SELECT 
                event_id::VARCHAR AS event_id,
                timestamp::TIMESTAMP AS event_timestamp,
                event_name::VARCHAR AS event_name,
                user_context.user_id::VARCHAR AS user_id,
                user_context.session_id::VARCHAR AS session_id,
                user_context.device::VARCHAR AS device,
                event_properties.product_id::VARCHAR AS product_id,
                event_properties.product_price::DOUBLE AS product_price,
                current_timestamp AS ingested_at
            FROM read_json_auto(
                '{json_files_pattern}',
                union_by_name=True,
                columns={{
                    event_id: 'VARCHAR',
                    timestamp: 'VARCHAR',
                    event_name: 'VARCHAR',
                    user_context: 'STRUCT(user_id VARCHAR, session_id VARCHAR, device VARCHAR, ip_address VARCHAR)',
                    event_properties: 'STRUCT(product_id VARCHAR, product_name VARCHAR, product_price DOUBLE, currency VARCHAR)',

                }}
            )
        ),
        deduplicated AS (
            SELECT *,
                   ROW_NUMBER() OVER(PARTITION BY event_id ORDER BY event_timestamp DESC) as rn
            FROM raw_extracted
        )
        SELECT 
            event_id, 
            event_timestamp, 
            event_name, 
            user_id, 
            session_id, 
            device, 
            product_id, 
            product_price,
            ingested_at
        FROM deduplicated 
        WHERE rn = 1;
    """
    con.execute(pipeline_query)

    # Create permanent target historical table if it doesn't exist
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver_events (
            event_id VARCHAR PRIMARY KEY,
            event_timestamp TIMESTAMP,
            event_name VARCHAR,
            user_id VARCHAR,
            session_id VARCHAR,
            device VARCHAR,
            product_id VARCHAR,
            product_price DOUBLE,
            ingested_at TIMESTAMP
        );
    """)

    # Merge staged records into permanent storage using an incremental upsert pattern
    upsert_query = """
        INSERT OR REPLACE INTO silver_events 
        SELECT * FROM staged_events;
    """
    con.execute(upsert_query)
    
    # Audit check to output progress metrics
    total_records = con.execute("SELECT COUNT(*) FROM silver_events;").fetchone()[0]
    print(f"[SUCCESS] Ingestion completed. Warehouse total: {total_records} unique tracking events.")
    
    con.close()

if __name__ == "__main__":
    run_ingestion_pipeline()