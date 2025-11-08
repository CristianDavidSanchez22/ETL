import boto3
from botocore import UNSIGNED
from botocore.config import Config

def list_files(bucket_name: str, prefix: str = "") -> list:
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            files.append(obj["Key"])
    return files

def download_one_file(bucket_name: str, s3_key: str, local_path: str):
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    s3.download_file(bucket_name, s3_key, local_path)
    print(f"Descargado: {s3_key} -> {local_path}")

if __name__ == "__main__":
    bucket = "s3-radaresideam"
    prefix = "l2_data/2024/05/15/"  # Cambia el prefijo si lo necesitas

    # Listar archivos
    files = list_files(bucket, prefix)
    if not files:
        print("No se encontraron archivos en el bucket con ese prefijo.")
    else:
        print("Archivos encontrados:")
        for i, f in enumerate(files):
            print(f"{i}: {f}")

        # Descargar el primer archivo encontrado
        s3_key = files[0]
        local_path = s3_key.split("/")[-1]
        download_one_file(bucket, s3_key, local_path)