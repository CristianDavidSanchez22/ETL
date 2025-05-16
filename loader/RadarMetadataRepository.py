import psycopg2
from psycopg2.extras import execute_values

class RadarMetadataRepository:
    def __init__(self, db_params: dict):
        self.db_params = db_params

    def get_processed_files(self, radar_name: str) -> set[str]:
        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT s3_key FROM radar_files WHERE radar_name = %s", (radar_name,))
                return {row[0] for row in cur.fetchall()}
    
    def insert_metadata_batch(self, records: list[dict]):
        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                values = [
                    (
                        r["radar_name"],
                        r["s3_key"],
                        r["processed_at"],
                        r["local_path"],
                        r["bbox"],
                        r["sweep_number"]
                    )
                    for r in records
                ]
                execute_values(cur, """
                    INSERT INTO radar_files (
                        radar_name, s3_key, processed_at,
                        local_path, bbox, sweep_number
                    )
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, values)
                conn.commit
