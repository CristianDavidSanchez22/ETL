import os
from datetime import datetime
import fsspec
import xarray as xr
import xradar as xd
import numpy as np
from shapely.geometry import box, mapping
import pytz
class RadarProcessor:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def process(self, s3_key: str) -> tuple[str, dict, dict]:
        # Leer archivo desde S3 con caché local
        remote_path = f"s3://s3-radaresideam/{s3_key}"
        file = fsspec.open_local(
            f"simplecache::{remote_path}",
            s3={"anon": True},
            filecache={"cache_storage": "./tmp_cache"}
        )

        # Abrir y procesar con xradar
        # ds = xr.open_dataset(file, engine="iris", group="sweep_0")
        # ds = xd.io.open_iris_datatree(file)['sweep_0'].ds
        info_radar = xd.io.open_iris_datatree(file)
        ds = info_radar['sweep_0'].ds
        ds.attrs.update(info_radar.attrs)
        ds = xd.georeference.get_x_y_z(ds)
        

        # Extraer timestamp y nombre del archivo
        timestamp = ds.time.values[0].astype("datetime64[s]").item().strftime("%Y%m%dT%H%M%S")
        filename = f"{timestamp}_sweep0.nc"
        local_path = os.path.join(self.output_dir, filename)
        ds.to_netcdf(local_path)

        # Extraer metadata y estadísticas
        metadata = self.get_metadata(ds, timestamp)
        statistics = self.get_statistics(ds)

        return local_path, metadata, statistics

    def get_metadata(self, ds: xr.Dataset, timestamp: str) -> dict:
        return {
            "timestamp": timestamp,
            "latitude": float(ds.latitude),
            "longitude": float(ds.longitude),
            "file_time": ds.time.values[0].astype("datetime64[s]").item(),
            "sweep_number": 0,
            "sweep_fixed_angle": float(ds.sweep_fixed_angle),
        }

    def get_statistics(self, ds: xr.Dataset) -> dict:
        variable = "DBZH"
        lima_tz = pytz.timezone("America/Lima")
        date = datetime.now(lima_tz)
        if variable not in ds.data_vars:
            raise ValueError(f"No se encontró la variable {variable} en el archivo.")

        data = ds[variable].values
        data = np.where(data == -9999, np.nan, data)  # Rellenar nodata si aplica

        mean_reflectivity = float(np.nanmean(data))
        max_reflectivity = float(np.nanmax(data))
        min_reflectivity = float(np.nanmin(data))
        rain_area_percent = float(np.sum(data > 20) / data.size * 100)
        try:
            tiempo = ds['time'].values
            duration_minutes = float((tiempo[-1] - tiempo[0]) / np.timedelta64(1, 'm'))
        except Exception:
            duration_minutes = None
        lon = float(ds.attrs.get('longitude', ds.get('longitude', 0)))
        lat = float(ds.attrs.get('latitude', ds.get('latitude', 0)))
        extent_deg = 0.5
        event_bbox = box(lon - extent_deg, lat - extent_deg, lon + extent_deg, lat + extent_deg)

        return {
            "mean_reflectivity": mean_reflectivity,
            "max_reflectivity": max_reflectivity,
            "min_reflectivity": min_reflectivity,
            "rain_area_percent": rain_area_percent,
            "duration_minutes": duration_minutes,
            "event_bbox": mapping(event_bbox),
            "created_at": date
        }
