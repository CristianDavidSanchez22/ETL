import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime

class S3RadarDownloader:
    def __init__(self, radar_name: str, bucket_name: str = "s3-radaresideam"):
        self.radar_name = radar_name
        self.bucket_name = bucket_name
        self.s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
        self.bucket = self.s3.Bucket(bucket_name)

    def list_files_for_date(self, date: datetime, extension=".RAWHDKV") -> list[str]:
        prefix = f"l2_data/{date.year}/{date.month:02d}/{date.day:02d}/{self.radar_name}/"
        return [
            obj.key for obj in self.bucket.objects.filter(Prefix=prefix)
            if obj.key.endswith(extension)
        ]
