import psycopg2
from psycopg2.extras import execute_values

class RadarMetadataRepository:
    _connection = None

    def __init__(self, db_params: dict):
        self.db_params = db_params
        if RadarMetadataRepository._connection is None:
            RadarMetadataRepository._connection = psycopg2.connect(**db_params)
        self.conn = RadarMetadataRepository._connection

    def close(self):
        if self.conn:
            self.conn.close()
            RadarMetadataRepository._connection = None

    def get_radar_id(self, radar_name: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM radar WHERE name = %s", (radar_name,))
            row = cur.fetchone()
            if row:
                return row[0]
            cur.execute("INSERT INTO radar (name) VALUES (%s) RETURNING id", (radar_name,))
            self.conn.commit()
            return cur.fetchone()[0]

    def get_processed_files(self, radar_name: str) -> set[str]:
        radar_id = self.get_radar_id(radar_name)
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT s3_key FROM radar_files WHERE radar_id = %s",
                (radar_id,)
            )
            return {row[0] for row in cur.fetchall()}

    def insert_metadata(self, record: dict):
        radar_id = self.get_radar_id(record["radar_name"])
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO radar_files (
                    radar_id, s3_key, processed_at,
                    local_path, bbox, sweep_number
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                radar_id,
                record["s3_key"],
                record["processed_at"],
                record["local_path"],
                record["bbox"],
                record["sweep_number"]
            ))
            self.conn.commit()