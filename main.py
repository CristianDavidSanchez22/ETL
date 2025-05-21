from RadarETL import RadarETL
from extractor.S3RadarDownloader import S3RadarDownloader
from processor.RadarProcessor import RadarProcessor
from loader.RadarMetadataRepository import RadarMetadataRepository


DB_PARAMS = {
    "dbname": "radar_ideam",
    "user": "postgres",
    "password": "123",
    "host": "localhost",
    "port": 5432
}

if __name__ == "__main__":
    downloader = S3RadarDownloader(radar_name="Guaviare")   
    processor = RadarProcessor(output_dir="./data/radar_processed")
    repository = RadarMetadataRepository(db_params=DB_PARAMS)

    etl = RadarETL(
        radar_name="Guaviare",
        downloader=downloader,
        processor=processor,
        repository=repository
    )
    etl.run()
