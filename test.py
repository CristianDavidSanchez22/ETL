import xarray as xr
import numpy as np
import matplotlib.pyplot as plt

def leer_y_graficar_reflectividad(nc_path, variable="DBZH"):
    ds = xr.open_dataset(nc_path)

    if variable not in ds.data_vars:
        print(f"No se encontró la variable {variable}")
        return
    dbz = ds[variable].values
    dbz = np.where(dbz == -9999, np.nan, dbz)

    lon0 = float(ds.attrs.get('longitude', ds.get('longitude', 0)))
    lat0 = float(ds.attrs.get('latitude', ds.get('latitude', 0)))
    azimuth = ds['azimuth'].values
    range_km = ds['range'].values / 1000

    azimuth_rad = np.deg2rad(azimuth)
    r, theta = np.meshgrid(range_km, azimuth_rad)
    x = r * np.sin(theta) / 111.32 + lon0
    y = r * np.cos(theta) / 110.57 + lat0

    fig = plt.figure(figsize=(8, 10))
    ax = plt.axes()
    ax.set_facecolor("none")
    ax.axis('off')

    # Ajusta los límites para que coincidan con los bounds de tu Home de React
    ax.set_xlim([-82, -66])
    ax.set_ylim([-5, 15])

    pcm = ax.pcolormesh(x, y, dbz, cmap='turbo', vmin=-30, vmax=40, shading='nearest')
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)
    plt.savefig("reflectividad_overlay.png", dpi=150, bbox_inches='tight', pad_inches=0, transparent=True)
    plt.close()

if __name__ == "__main__":
    archivo = r"F:\Archivos\PC\Maestria\Tesis\Daza\data\radar_processed\20250612T000346_sweep0.nc"
    leer_y_graficar_reflectividad(archivo)