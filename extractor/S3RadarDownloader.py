import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime
import re
class S3RadarDownloader:
    def __init__(self, radar_name: str, bucket_name: str = "s3-radaresideam"):
        self.radar_name = radar_name
        self.bucket_name = bucket_name
        self.s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        self.bucket = self.s3.Bucket(bucket_name)

    def list_files_for_date(self, date: datetime, extension=".RAWHDKV", from_hour: int = 0) -> list[str]:

        prefix = f"l2_data/{date.year}/{date.month:02d}/{date.day:02d}/{self.radar_name}/"
        files = []
        for obj in self.bucket.objects.filter(Prefix=prefix):
            key = obj.key

            # Extrae el timestamp del nombre del archivo: GUA250616120054 → 250616120054
            match = re.search(r"(\d{12})", key)
            if match:
                ts_str = match.group(1)  # YYMMDDHHMMSS
                hour = int(ts_str[6:8])  # HH

                if hour >= from_hour:
                    files.append(key)

        return files