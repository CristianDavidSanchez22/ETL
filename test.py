import os
import xarray as xr
import xradar as xd
from pandas import to_datetime 


def process_local_file(local_path: str, output_dir: str) -> tuple[str, dict]:
    os.makedirs(output_dir, exist_ok=True)

    # Abrir y procesar con xradar
    ds = xr.open_dataset(local_path, engine="iris", group="sweep_0", decode_cf=False)
    ds = xd.georeference.get_x_y_z(ds)


    # Extraer timestamp y nombre del archivo
    timestamp = to_datetime(ds.time.values[0])
    filename = f"{timestamp}_sweep0.nc"
    output_path = os.path.join(output_dir, filename)
    ds.to_netcdf(output_path)

    # Extraer metadata
    metadata = {
        "timestamp": timestamp,
        "latitude": float(ds.latitude),
        "longitude": float(ds.longitude),
        "sweep_number": 0
    }

    print(f"Archivo procesado y guardado en: {output_path}")
    print("Metadata extraída:", metadata)
    return output_path, metadata

if __name__ == "__main__":
    # Cambia estos valores según tu archivo descargado
    local_file = "GUA240515011924.RAWDCHH"
    output_dir = "./procesados"

    process_local_file(local_file, output_dir)