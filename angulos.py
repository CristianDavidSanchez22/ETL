import xarray as xr
import glob
import os

def listar_sweep_angles_y_tipo(folder_path, angle_var="sweep_fixed_angle", tipo_var="scan_name"):
    archivos = sorted(glob.glob(os.path.join(folder_path, "*.nc")))
    if not archivos:
        print("No se encontraron archivos NetCDF en la carpeta.")
        return

    print(f"{'Archivo':60} | {angle_var:20} | {tipo_var}")
    print("-" * 100)
    for archivo in archivos:
        try:
            ds = xr.open_dataset(archivo)
            # Sweep angle
            if angle_var in ds.variables:
                angle = ds[angle_var].values.item() if ds[angle_var].size == 1 else ds[angle_var].values
            else:
                angle = "No encontrado"
            # Sweep type/name 
            tipo = ds.attrs.get(tipo_var, "No encontrado")
            print(f"{os.path.basename(archivo):60} | {str(angle):20} | {tipo}")
        except Exception as e:
            print(f"{os.path.basename(archivo):60} | Error: {e}")

if __name__ == "__main__":
    listar_sweep_angles_y_tipo("./data/radar_processed")