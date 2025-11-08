from RadarETL import RadarETL
from extractor.S3RadarDownloader import S3RadarDownloader
from processor.RadarProcessor import RadarProcessor
from loader.RadarMetadataRepository import RadarMetadataRepository
import warnings
import numpy as np
import os
import argparse
from datetime import datetime
from pytz import timezone

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

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--radar", type=str, required=False, default="Guaviare", help="Nombre del radar a procesar (default: Guaviare)")
    parser.add_argument("--date", type=str, required=False, help="Fecha en formato YYYYMMDD para procesar archivos específicos")
    parser.add_argument("--hour", type=int, required=False, default= 0, help="Hora en formato HH para procesar archivos específicos desde hora especifica")
    args = parser.parse_args()
    radar_name = args.radar
    from_hour = args.hour
    if args.date:
        input_date = args.date
    else:
        lima_tz = timezone("America/Lima")
        input_date = datetime.now(lima_tz).strftime("%Y%m%d")
    return radar_name, input_date, from_hour



if __name__ == "__main__":
    
    radar_name, input_date,from_hour = parse_args()
        
    downloader = S3RadarDownloader(radar_name)   
    processor = RadarProcessor(output_dir=os.path.abspath(f"./data/radar_processed/{radar_name}/{input_date}/"))
    repository = RadarMetadataRepository(db_url=DB_URL)
    input_date = datetime.strptime(input_date, "%Y%m%d") if input_date else None
    etl = RadarETL(
        radar_name=radar_name,
        downloader=downloader,
        processor=processor,
        repository=repository
    )
    etl.run(input_date,from_hour)
