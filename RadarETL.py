import logging
from datetime import datetime
import pytz

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

class RadarETL:
    def __init__(self, radar_name: str, downloader, processor, repository):
        self.radar_name = radar_name
        self.downloader = downloader
        self.processor = processor
        self.repository = repository

    def run(self, date: datetime = None):
        lima_tz = pytz.timezone("America/Lima")
        date = date or datetime.now(lima_tz)
        logging.info(f"Checking files for {self.radar_name} on {date.date()}")

        all_files = self.downloader.list_files_for_date(date)
        processed_files = self.repository.get_processed_files(self.radar_name)

        new_files = [f for f in all_files if f not in processed_files]
        if not new_files:
            logging.info("No new files to process.")
            return
 
        metadata_records = []
        for s3_key in new_files:
            try:
                local_path, meta = self.processor.process(s3_key)
                metadata = {
                    "radar_name": self.radar_name,
                    "s3_key": s3_key,
                    "processed_at": date,
                    "local_path": local_path,
                    "bbox": f"SRID=4326;POINT({meta['longitude']} {meta['latitude']})",
                    "sweep_number": meta["sweep_number"]
                }
                metadata_records.append(metadata)
                logging.info(f"Processed {s3_key} -> {local_path}")
                break 
            except Exception as e:
                logging.error(f"Error processing {s3_key}: {e}")

        if metadata_records:
            self.repository.insert_metadata_batch(metadata_records)
            logging.info(f"Inserted {len(metadata_records)} records into DB.")
