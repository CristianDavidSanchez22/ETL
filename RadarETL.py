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

        try:
            for s3_key in new_files:
                try:
                    local_path, meta, statistics = self.processor.process(s3_key)
                    metadata = {
                        "radar_name": self.radar_name,
                        "s3_key": s3_key,
                        "processed_at": date,
                        "file_time": meta['file_time'],
                        "local_path": local_path,
                        "bbox": f"SRID=4326;POINT({meta['longitude']} {meta['latitude']})",
                        "sweep_fixed_angle": f"{meta['sweep_fixed_angle']}"
                    }
                    radar_file_id = self.repository.insert_metadata(metadata)
                    self.repository.insert_statistics(radar_file_id, statistics)
                    logging.info(f"Processed {s3_key} -> {local_path}")
                except Exception as e:
                    logging.error(f"Error processing {s3_key}: {e}")
        finally:
            self.repository.close()
            logging.info("ETL process completed.")
