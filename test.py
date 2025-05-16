import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def list_sigmet_files(radar_name: str, date: datetime):
    s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
    bucket = s3.Bucket("s3-radaresideam")

    prefix = f"l2_data/{date.year}/{date.month:02d}/{date.day:02d}/{radar_name}/"
    files = [
        obj.key for obj in bucket.objects.filter(Prefix=prefix)
    ]

    logging.info(f"{len(files)} archivos encontrados para {radar_name} el {date.date()}")
    for key in files:
        logging.debug(f" - {key}")

    return files


if __name__ == "__main__":
    try:
        date = datetime(2025, 5, 15)
    except ValueError:
        logging.error("Formato de fecha inválido. Usa YYYY-MM-DD.")
        exit(1)

    list_sigmet_files("Guaviare", date)
