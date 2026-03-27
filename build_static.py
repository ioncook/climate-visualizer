import os
import json
import io
import math
import numpy as np
import netCDF4
from PIL import Image
import colorsys
import threading
import concurrent.futures

# --- CONFIG ---
Z_MAX = 6
OUT_DIR = 'docs'

print("Initializing Static GitHub Pages Builder...")

# 1. LOAD DATA
print("Loading Master NetCDF Dataset (39MB)...")
ds = netCDF4.Dataset('Current data/koppen_geiger_nc/1991_2020/koppen_geiger_0p00833333.nc')
var_name = [v for v in ds.variables.keys() if 'kg_class' in v.lower()][0]
master_data = np.nan_to_num(ds.variables[var_name][:], 0).astype(np.uint8)
H, W = master_data.shape
ds.close()

print("Loading Ensemble Mean Data (56MB)...")
ds_ens = netCDF4.Dataset('Current data/climate_data_0p1/1991_2020/ensemble_mean_0p1.nc')
temp_data = np.nan_to_num(ds_ens.variables['air_temperature'][:], 0)
precip_data = np.nan_to_num(ds_ens.variables['precipitation'][:], 0)
E_H, E_W = temp_data.shape[1], temp_data.shape[2]
ds_ens.close()

mean_precip = np.mean(precip_data, axis=0)

# 2. COLOR LUTS
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

color_lut = np.zeros((32, 4), dtype=np.uint8)
for k, v in COLORS.items():
    color_lut[k] = [v[0], v[1], v[2], 200]

precip_lut = np.zeros((501, 4), dtype=np.uint8)
for p in range(501):
    curve = (p / 500) ** 0.7
    hue = (270 * curve) / 360.0 
    r, g, b = colorsys.hls_to_rgb(hue, 0.65, 1.0)
    precip_lut[p] = [int(r*255), int(g*255), int(b*255), 200]

mean_temp = np.mean(temp_data, axis=0)
temp_lut = np.zeros((100, 4), dtype=np.uint8)
for t in range(-40, 60):
    clamped = max(-30, min(30, t))
    hue = 270 * (1 - (clamped + 30) / 60)
    r, g, b = colorsys.hls_to_rgb(hue / 360.0, 0.65, 1.0)
    temp_lut[t + 40] = [int(r*255), int(g*255), int(b*255), 200]


# 3. BUILD QUERY DATABASE (Sparse JSON of Land data at 0.5 degrees)
print("Building Static Global Query Database...")
# Load 0.5 degree data just for the JSON to keep it small
ds_ens_05 = netCDF4.Dataset('Current data/climate_data_0p5/1991_2020/ensemble_mean_0p5.nc')
t_05 = np.nan_to_num(ds_ens_05.variables['air_temperature'][:], 0)
p_05 = np.nan_to_num(ds_ens_05.variables['precipitation'][:], 0)

lat_len = t_05.shape[1]
lon_len = t_05.shape[2]

query_db = {}
# Only export where precip is > 0 (land roughly)
# To be safe, we check if the 1km master mask says land
for y in range(lat_len):
    for x in range(lon_len):
        lat = 90 - (y * 180 / lat_len)
        lon = (x * 360 / lon_len) - 180
        
        y_1km = min(int((90 - lat) / 180 * H), H - 1)
        x_1km = min(int((lon + 180) / 360 * W), W - 1)
        
        kid = int(master_data[y_1km, x_1km])
        
        if kid > 0:  # Valid land
            temps = [round(float(t_05[m, y, x]), 1) for m in range(12)]
            precips = [int(round(float(p_05[m, y, x]))) for m in range(12)]
            
            # Key is tightly packed "y_x"
            query_db[f"{y}_{x}"] = {
                "i": kid,
                "t": temps,
                "p": precips
            }

ds_ens_05.close()

os.makedirs(OUT_DIR, exist_ok=True)
with open(os.path.join(OUT_DIR, 'query_db.json'), 'w') as f:
    json.dump(query_db, f, separators=(',', ':'))

print(f"Query Database saved! Exported {len(query_db)} land coordinates.")


# 4. TILE GENERATION FUNCTIONS
def _gen_arrays(z, x, y):
    num_tiles = 2 ** z
    map_size = 256 * num_tiles

    px = x * 256 + np.arange(256)
    x_norm = (px / map_size) - 0.5
    lons = 360 * x_norm
    
    py = y * 256 + np.arange(256)
    y_norm = 0.5 - (py / map_size)
    lats = 90 - 360 * np.arctan(np.exp(-y_norm * 2 * np.pi)) / np.pi
    
    return lons, lats

def process_tile(args):
    z, x, y = args
    
    # KOPPEN
    koppen_dir = os.path.join(OUT_DIR, f'tiles_koppen/{z}/{x}')
    os.makedirs(koppen_dir, exist_ok=True)
    koppen_path = os.path.join(koppen_dir, f'{y}.png')
    
    # Check if all files exist (koppen, 13 temp, 13 precip)
    all_exist = os.path.exists(koppen_path)
    if all_exist:
        for m in range(13):
            if not os.path.exists(os.path.join(OUT_DIR, f'tiles_temp/{m}/{z}/{x}/{y}.png')) or \
               not os.path.exists(os.path.join(OUT_DIR, f'tiles_precip/{m}/{z}/{x}/{y}.png')):
                all_exist = False
                break
    if all_exist:
        return True

    lons, lats = _gen_arrays(z, x, y)
    
    # Indexes
    lon_idx = np.floor((lons + 180) / 360 * E_W).astype(int) % E_W
    lon_1km = np.floor((lons + 180) / 360 * W).astype(int) % W
    
    lat_idx_raw = np.floor((90 - lats) / 180 * E_H).astype(int)
    lat_1km_raw = np.floor((90 - lats) / 180 * H).astype(int)
    
    lat_idx = np.clip(lat_idx_raw, 0, E_H - 1)
    lat_1km = np.clip(lat_1km_raw, 0, H - 1)
    
    LON_IDX, LAT_IDX = np.meshgrid(lon_idx, lat_idx)
    LON_1KM, LAT_1KM = np.meshgrid(lon_1km, lat_1km)
    
    LAT_VALID = (lat_idx_raw >= 0) & (lat_idx_raw < E_H)
    _, LAT_VALID_GRID = np.meshgrid(lon_idx, LAT_VALID)
    
    water_mask = master_data[LAT_1KM, LON_1KM] == 0
    void_mask = np.logical_not(LAT_VALID_GRID)

    # 1. KOPPEN IMAGE
    if not os.path.exists(koppen_path):
        tile_classes = master_data[LAT_1KM, LON_1KM]
        img_k = color_lut[tile_classes]
        img_k[void_mask] = [0, 0, 0, 0]
        Image.fromarray(img_k, 'RGBA').save(koppen_path)

    # 2. PRECIP IMAGES
    for m in range(13):
        precip_m_dir = os.path.join(OUT_DIR, f'tiles_precip/{m}/{z}/{x}')
        os.makedirs(precip_m_dir, exist_ok=True)
        precip_m_path = os.path.join(precip_m_dir, f'{y}.png')

        if not os.path.exists(precip_m_path):
            if m == 12:
                tile_precips = mean_precip[LAT_IDX, LON_IDX]
            else:
                tile_precips = precip_data[m, LAT_IDX, LON_IDX]
                
            clamped_p = np.clip(tile_precips, 0, 500).astype(int)
            img_p = precip_lut[clamped_p].copy()
            
            img_p[water_mask] = [0, 0, 0, 0]
            img_p[void_mask] = [0, 0, 0, 0]
            Image.fromarray(img_p, 'RGBA').save(precip_m_path)

    # 3. TEMP IMAGES
    for m in range(13):
        temp_dir = os.path.join(OUT_DIR, f'tiles_temp/{m}/{z}/{x}')
        os.makedirs(temp_dir, exist_ok=True)
        temp_path = os.path.join(temp_dir, f'{y}.png')
        
        if not os.path.exists(temp_path):
            if m == 12:
                tile_temps = mean_temp[LAT_IDX, LON_IDX]
            else:
                tile_temps = temp_data[m, LAT_IDX, LON_IDX]
                
            clamped_t = np.clip(tile_temps, -40, 59).astype(int) + 40
            img_t = temp_lut[clamped_t].copy()
            
            img_t[water_mask] = [0, 0, 0, 0]
            img_t[void_mask] = [0, 0, 0, 0]
            Image.fromarray(img_t, 'RGBA').save(temp_path)

    return True

print("Generating Static Map Tiles...")
tiles_to_make = [(z, x, y) for z in range(Z_MAX + 1) for x in range(2**z) for y in range(2**z)]

# Run sequentially without multiprocessing pool to avoid nested multiprocessing locks
for t in tiles_to_make:
    process_tile(t)

print("STATIC GITHUB PAGES COMPILE SUCCESSFUL!")
