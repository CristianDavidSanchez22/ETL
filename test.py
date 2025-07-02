import xarray as xr
import xradar as xd

def mostrar_metadatos_raw(path):
    # Intenta abrir el archivo con xradar (engine="iris")
    try:
        ds = xr.open_dataset(path, engine="iris", group="sweep_0")
    except Exception as e:
        print(f"Error al abrir el archivo: {e}")
        return

    print("Atributos globales:")
    for k, v in ds.attrs.items():
        print(f"  {k}: {v}")

    print("\nVariables:")
    for var in ds.variables:
        print(f"  {var}: {ds[var].dims}, dtype={ds[var].dtype}")

    print("\nCoordenadas:")
    for coord in ds.coords:
        print(f"  {coord}: {ds[coord].dims}, dtype={ds[coord].dtype}")

if __name__ == "__main__":
    archivo = r"C:\Users\Ryzen\Desktop\Maestry\TESIS\Nueva carpeta\ETL\data\radar_processed\Guaviare\20250510\20250510T000127_sweep0.nc"
    mostrar_metadatos_raw(archivo)
    
    # C:\Users\Ryzen\Desktop\Maestry\TESIS\Nueva carpeta\ETL\data\radar_processed\Tablazo\20250611\20250611T000012_sweep0.nc