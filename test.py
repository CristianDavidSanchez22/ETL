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
    archivo = r"F:\Archivos\PC\Maestria\Tesis\Daza\GUA240501000024.RAWA1T5"
    mostrar_metadatos_raw(archivo)