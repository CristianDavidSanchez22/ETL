from RadarETL import RadarETL
from extractor.S3RadarDownloader import S3RadarDownloader
from processor.RadarProcessor import RadarProcessor
from loader.RadarMetadataRepository import RadarMetadataRepository


DB_PARAMS = {
    "dbname": "radares_ideam",
    "user": "postgres",
    "password": "123",
    "host": "localhost",
    "port": 5432
}
DB_URL = f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"

if __name__ == "__main__":
    downloader = S3RadarDownloader(radar_name="Santa Elena SIATA")   
    processor = RadarProcessor(output_dir="./data/radar_processed")
    repository = RadarMetadataRepository(db_url=DB_URL)

    etl = RadarETL(
        radar_name="Santa Elena SIATA",
        downloader=downloader,
        processor=processor,
        repository=repository
    )
    etl.run()
