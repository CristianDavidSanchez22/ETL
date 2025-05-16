import os
from datetime import datetime
import fsspec
import xarray as xr
import xradar as xd

class RadarProcessor:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def process(self, s3_key: str) -> tuple[str, dict]:
        # Leer archivo desde S3 con caché local
        remote_path = f"s3://s3-radaresideam/{s3_key}"
        file = fsspec.open_local(
            f"simplecache::{remote_path}",
            s3={"anon": True},
            filecache={"cache_storage": "./tmp_cache"}
        )

        # Abrir y procesar con xradar
        ds = xr.open_dataset(file, engine="iris", group="sweep_0")
        ds = xd.georeference.get_x_y_z(ds)

        # Extraer timestamp y nombre del archivo
        timestamp = ds.time.values.astype("datetime64[s]").item().strftime("%Y%m%dT%H%M%S")
        filename = f"{timestamp}_sweep0.nc"
        local_path = os.path.join(self.output_dir, filename)
        ds.to_netcdf(local_path)

        # Extraer metadata
        metadata = {
            "timestamp": timestamp,
            "latitude": float(ds.latitude),
            "longitude": float(ds.longitude),
            "sweep_number": 0
        }

        return local_path, metadata
