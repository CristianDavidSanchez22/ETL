import os
import logging
from datetime import datetime
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import fsspec
import xarray as xr
import xradar as xd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

class RadarETL:
    def __init__(self, radar_name, output_dir, db_params):
        self.radar_name = radar_name
        self.output_dir = output_dir
        self.db_params = db_params
        self.s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        self.bucket = self.s3.Bucket("s3-radaresideam")

    def list_new_files(self, date: datetime):
        prefix = f"l2_data/{date.year}/{date.month:02d}/{date.day:02d}/{self.radar_name}/"
        return [
            obj.key for obj in self.bucket.objects.filter(Prefix=prefix)
        ]

    def filter_unprocessed(self, files):
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        cur.execute("SELECT s3_key FROM radar_files WHERE radar_name = %s", (self.radar_name,))
        processed = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()
        return [f for f in files if f not in processed]

    def process_and_save(self, s3_key):
        remote_path = f"s3://s3-radaresideam/{s3_key}"
        file = fsspec.open_local(
            f"simplecache::{remote_path}",
            s3={"anon": True},
            filecache={"cache_storage": "./tmp_cache"}
        )

        ds = xr.open_dataset(file, engine="iris", group="sweep_0")
        ds = xd.georeference.get_x_y_z(ds)

        timestamp = ds.time.values.astype("datetime64[s]").item().strftime("%Y%m%dT%H%M%S")
        path = os.path.join(self.output_dir, f"{self.radar_name}_{timestamp}_sweep0.nc")
        ds.to_netcdf(path)

        lat = float(ds.latitude)
        lon = float(ds.longitude)

        return path, {
            "radar_name": self.radar_name,
            "s3_key": s3_key,
            "processed_at": datetime.utcnow(),
            "local_path": path,
            "bbox": f"SRID=4326;POINT({lon} {lat})",
            "sweep_number": 0
        }

    def save_metadata(self, records):
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        values = [
            (r["radar_name"], r["s3_key"], r["processed_at"], r["local_path"], r["bbox"], r["sweep_number"])
            for r in records
        ]
        execute_values(cur, """
            INSERT INTO radar_files (radar_name, s3_key, processed_at, local_path, bbox, sweep_number)
            VALUES %s ON CONFLICT DO NOTHING
        """, values)
        conn.commit()
        cur.close()
        conn.close()

    def run(self):
        today = datetime.utcnow()
        logging.info(f"Checking files for {self.radar_name} on {today.date()}")
        files = self.list_new_files(today)
        new_files = self.filter_unprocessed(files)

        if not new_files:
            logging.info("No new files to process.")
            return

        records = []
        for key in new_files:
            try:
                path, meta = self.process_and_save(key)
                records.append(meta)
                logging.info(f"Processed {key} -> {path}")
            except Exception as e:
                logging.error(f"Error processing {key}: {e}")

        if records:
            self.save_metadata(records)
            logging.info(f"Inserted {len(records)} records into DB.")


if __name__ == "__main__":
    DB_PARAMS = {
        "dbname": "radar",
        "user": "etl",
        "password": "yourpassword",
        "host": "localhost",
        "port": 5432
    }

    etl = RadarETL(
        radar_name="Guaviare",
        output_dir="/data/radar_processed",
        db_params=DB_PARAMS
    )
    etl.run()
