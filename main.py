from RadarETL import RadarETL
from extractor.S3RadarDownloader import S3RadarDownloader
from processor.RadarProcessor import RadarProcessor
from loader.RadarMetadataRepository import RadarMetadataRepository
import warnings
import numpy as np
import os

warnings.filterwarnings("ignore", category=RuntimeWarning)
np.seterr(invalid='ignore')

DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", "radares_ideam"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "123"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432))
}

DB_URL = f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"

if __name__ == "__main__":
    downloader = S3RadarDownloader(radar_name="Guaviare")   
    processor = RadarProcessor(output_dir="./data/radar_processed")
    repository = RadarMetadataRepository(db_url=DB_URL)

    etl = RadarETL(
        radar_name="Guaviare",
        downloader=downloader,
        processor=processor,
        repository=repository
    )
    etl.run()
