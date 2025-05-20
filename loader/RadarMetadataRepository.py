import psycopg2
from psycopg2.extras import execute_values

class RadarMetadataRepository:
    def __init__(self, db_params: dict):
        self.db_params = db_params

    def get_radar_id(self, radar_name: str) -> int:
        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM radar WHERE name = %s", (radar_name,))
                row = cur.fetchone()
                if row:
                    return row[0]
                cur.execute("INSERT INTO radar (name) VALUES (%s) RETURNING id", (radar_name,))
                return cur.fetchone()[0]

    def get_processed_files(self, radar_name: str) -> set[str]:
        radar_id = self.get_radar_id(radar_name)
        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT s3_key FROM radar_files WHERE radar_id = %s",
                    (radar_id,)
                )
                return {row[0] for row in cur.fetchall()}

    def insert_metadata_batch(self, records: list[dict]):
        if not records:
            return
        radar_name = records[0]["radar_name"]
        radar_id = self.get_radar_id(radar_name)
        with psycopg2.connect(**self.db_params) as conn:
            with conn.cursor() as cur:
                values = [
                    (
                        radar_id,
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
                        radar_id, s3_key, processed_at,
                        local_path, bbox, sweep_number
                    )
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, values)
                conn.commit()