import os
import netCDF4
import numpy as np
import math
from PIL import Image

# Configuration
Z_MAX = 5  # Zoom levels 0 to 5
NC_FILE = 'Current data/koppen_geiger_nc/1991_2020/koppen_geiger_0p1.nc'
OUT_DIR = 'tiles/1991_2020'

# Colors exactly from Beck et al. / koppen.earth
COLORS = {
    1: [0, 0, 255], 2: [0, 120, 255], 3: [70, 170, 250], 4: [255, 0, 0],
    5: [255, 150, 150], 6: [245, 165, 0], 7: [255, 220, 100], 8: [255, 255, 0],
    9: [200, 200, 0], 10: [150, 150, 0], 11: [150, 255, 150], 12: [100, 200, 100],
    13: [50, 150, 50], 14: [200, 255, 80], 15: [100, 255, 80], 16: [50, 200, 0],
    17: [255, 0, 255], 18: [200, 0, 200], 19: [150, 50, 150], 20: [150, 100, 150],
    21: [170, 175, 255], 22: [90, 120, 220], 23: [75, 80, 180], 24: [50, 0, 135],
    25: [0, 255, 255], 26: [55, 200, 255], 27: [0, 125, 125], 28: [0, 70, 95],
    29: [178, 178, 178], 30: [102, 102, 102]
}

# Create color lookup table (0 will be transparent)
color_lut = np.zeros((32, 4), dtype=np.uint8)
for k, v in COLORS.items():
    color_lut[k] = [v[0], v[1], v[2], 180]  # RGBA, 180 alpha

print("Loading NetCDF...")
ds = netCDF4.Dataset(NC_FILE)
var_name = [v for v in ds.variables.keys() if 'kg_class' in v.lower()][0]
data = ds.variables[var_name][:]
ds.close()

data = np.nan_to_num(data, 0).astype(np.uint8)

print(f"Data shape: {data.shape}")

# Precompute Web Mercator lookups to avoid math inside the loop
# We can just process tiles sequentially.
for z in range(Z_MAX + 1):
    print(f"Generating Zoom Level {z}...")
    num_tiles = 2 ** z
    map_size = 256 * num_tiles

    for x in range(num_tiles):
        tile_dir = os.path.join(OUT_DIR, str(z), str(x))
        os.makedirs(tile_dir, exist_ok=True)
        
        px = x * 256 + np.arange(256)
        x_norm = (px / map_size) - 0.5
        lons = 360 * x_norm
        lon_idx = np.floor((lons + 180) * 10).astype(int)
        lon_idx = np.clip(lon_idx, 0, data.shape[1] - 1)
        
        for y in range(num_tiles):
            out_path = os.path.join(tile_dir, f"{y}.png")
            if os.path.exists(out_path): 
                continue
                
            py = y * 256 + np.arange(256)
            y_norm = 0.5 - (py / map_size)
            lats = 90 - 360 * np.arctan(np.exp(-y_norm * 2 * np.pi)) / np.pi
            lat_idx = np.floor((90 - lats) * 10).astype(int)
            lat_idx = np.clip(lat_idx, 0, data.shape[0] - 1)
            
            # Create meshgrid of indices
            LON_IDX, LAT_IDX = np.meshgrid(lon_idx, lat_idx)
            
            tile_classes = data[LAT_IDX, LON_IDX]
            
            if not np.any(tile_classes):
                continue

            rgba_img = color_lut[tile_classes]
            img = Image.fromarray(rgba_img, 'RGBA')
            img.save(out_path)
            
print("All static tiles generated successfully!")
