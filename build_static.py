import os
import json
import math
import numpy as np
import netCDF4
from PIL import Image
import colorsys
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import gc
from scipy import ndimage

# --- CONFIG ---
Z_MAX = 7
OUT_DIR = 'docs'
ERAS = ["1901_1930", "1931_1960", "1961_1990", "1991_2020"]
MAX_WORKERS = 2 # Strictly limited to 2 workers for memory stability

# Global caches for workers
_color_lut = None
_precip_lut = None
_temp_lut = None
_era_data = None # (t_data, p_data, mean_temp, mean_precip, master_data, master_shape)

def init_worker(colors_dict):
    global _color_lut, _precip_lut, _temp_lut
    _color_lut = np.zeros((32, 4), dtype=np.uint8)
    for k, v in colors_dict.items():
        _color_lut[k] = [v[0], v[1], v[2], 255]
    _color_lut[0] = [0, 0, 0, 0]

    _precip_lut = np.zeros((501, 4), dtype=np.uint8)
    for p in range(501):
        curve = (p / 500) ** 0.7
        hue = (270 * curve) / 360.0 
        r, g, b = colorsys.hls_to_rgb(hue, 0.65, 1.0)
        _precip_lut[p] = [int(r*255), int(g*255), int(b*255), 255]

    _temp_lut = np.zeros((100, 4), dtype=np.uint8)
    for t in range(-40, 60):
        clamped = max(-30, min(30, t))
        hue = 270 * (1 - (clamped + 30) / 60)
        r, g, b = colorsys.hls_to_rgb(hue / 360.0, 0.65, 1.0)
        _temp_lut[t + 40] = [int(r*255), int(g*255), int(b*255), 255]

def fill_missing(data, invalid=None):
    # Fill zeros/NaNs using nearest neighbor extrapolation from the nearest valid data points
    # We treat absolute 0 as invalid to catch shoreline artifacts where mask data is missing
    if invalid is None:
        invalid = (data == 0) | np.isnan(data)
    else:
        # Augment the mask with zero-check to catch source artifacts
        invalid = invalid | (data == 0)
        
    if not np.any(invalid): return data
    ind = ndimage.distance_transform_edt(invalid, return_distances=False, return_indices=True)
    return data[tuple(ind)]

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

def process_tile_task(args):
    z, x, y, era_name, ensemble_path, koppen_path_data = args
    global _era_data
    
    # Load era data once per process per era
    if _era_data is None or _era_data['path'] != ensemble_path:
        _era_data = None 
        gc.collect()
        
        ds_e = netCDF4.Dataset(ensemble_path)
        v_t = ds_e.variables['air_temperature']
        v_p = ds_e.variables['precipitation']
        
        t = v_t[:]
        np.nan_to_num(t, copy=False, nan=0.0)
        p = v_p[:]
        np.nan_to_num(p, copy=False, nan=0.0)
        
        # Extrapolate data into ocean for cleaner masking at the coastline
        for m in range(t.shape[0]):
            t[m] = fill_missing(t[m], v_t[m].mask if hasattr(v_t, 'mask') else None)
            p[m] = fill_missing(p[m], v_p[m].mask if hasattr(v_p, 'mask') else None)
            
        ds_e.close()
        
        # Load Era-Specific Köppen
        ds_k = netCDF4.Dataset(koppen_path_data)
        v_k = [v for v in ds_k.variables.keys() if 'kg_class' in v.lower()][0]
        m_k = np.nan_to_num(ds_k.variables[v_k][:], 0).astype(np.uint8)
        m_s = m_k.shape
        ds_k.close()

        _era_data = {
            'path': ensemble_path,
            't': t, 'p': p,
            'mT': np.mean(t, axis=0), 'mP': np.mean(p, axis=0),
            'shape': (t.shape[1], t.shape[2]),
            'master': m_k, 'm_shape': m_s
        }
        gc.collect()

    t_data, p_data = _era_data['t'], _era_data['p']
    mean_temp, mean_precip = _era_data['mT'], _era_data['mP']
    master_data, master_shape = _era_data['master'], _era_data['m_shape']
    E_H, E_W = _era_data['shape']
    H, W = master_shape

    era_dir = os.path.join(OUT_DIR, era_name)
    koppen_tile_path = os.path.join(era_dir, f'tiles_koppen/{z}/{x}/{y}.png')
    
    os.makedirs(os.path.dirname(koppen_tile_path), exist_ok=True)
    lons, lats = _gen_arrays(z, x, y)
    lon_idx = np.floor((lons + 180) / 360 * E_W).astype(int) % E_W
    lon_1km = np.floor((lons + 180) / 360 * W).astype(int) % W
    lat_idx_raw = np.floor((90 - lats) / 180 * E_H).astype(int)
    lat_1km_raw = np.floor((90 - lats) / 180 * H).astype(int)
    lat_idx, lat_1km = np.clip(lat_idx_raw, 0, E_H - 1), np.clip(lat_1km_raw, 0, H - 1)
    
    LON_IDX, LAT_IDX = np.meshgrid(lon_idx, lat_idx)
    LON_1KM, LAT_1KM = np.meshgrid(lon_1km, lat_1km)
    LAT_VALID = (lat_idx_raw >= 0) & (lat_idx_raw < E_H)
    _, LAT_VALID_GRID = np.meshgrid(lon_idx, LAT_VALID)
    
    water_mask = master_data[LAT_1KM, LON_1KM] == 0
    void_mask = np.logical_not(LAT_VALID_GRID)

    # 1. KOPPEN
    # NEVER skip here if we want to be sure, or just rely on manual deletion
    if not os.path.exists(koppen_tile_path):
        tile_classes = master_data[LAT_1KM, LON_1KM]
        img_k = _color_lut[tile_classes]
        img_k[void_mask] = [0, 0, 0, 0]
        # Only save if there is at least one non-transparent pixel
        if np.any(img_k[:,:,3] > 0):
            Image.fromarray(img_k, 'RGBA').save(koppen_tile_path)

    # Generate all months (0-12) for modern era, but only annual (12) for others
    months_to_gen = range(13) if era_name == "1991_2020" else [12]
    for m in months_to_gen:
        p_p = os.path.join(era_dir, f'tiles_precip/{m}/{z}/{x}/{y}.png')
        t_p = os.path.join(era_dir, f'tiles_temp/{m}/{z}/{x}/{y}.png')
        os.makedirs(os.path.dirname(p_p), exist_ok=True)
        os.makedirs(os.path.dirname(t_p), exist_ok=True)

        if not os.path.exists(p_p):
            vals = mean_precip[LAT_IDX, LON_IDX] if m == 12 else p_data[m, LAT_IDX, LON_IDX]
            img = _precip_lut[np.clip(vals, 0, 500).astype(int)].copy()
            img[water_mask] = img[void_mask] = [0, 0, 0, 0]
            if np.any(img[:,:,3] > 0):
                Image.fromarray(img, 'RGBA').save(p_p)
        
        if not os.path.exists(t_p):
            vals = mean_temp[LAT_IDX, LON_IDX] if m == 12 else t_data[m, LAT_IDX, LON_IDX]
            img = _temp_lut[np.clip(vals, -40, 59).astype(int) + 40].copy()
            img[water_mask] = img[void_mask] = [0, 0, 0, 0]
            if np.any(img[:,:,3] > 0):
                Image.fromarray(img, 'RGBA').save(t_p)
            
    # 3. KOPPEN RLE JSON TILE (Z=7 Optimization)
    if z == 7:
        q_dir = os.path.join(era_dir, 'koppen_rle', str(z), str(x))
        os.makedirs(q_dir, exist_ok=True)
        q_p = os.path.join(q_dir, f"{y}.json")
        
        if not os.path.exists(q_p):
            tile_classes = master_data[LAT_1KM, LON_1KM]
            # Use RLE rows to maximize swath compression
            rle_rows = []
            for row in tile_classes:
                packed = []
                if np.all(row == row[0]):
                    packed = [[256, int(row[0])]]
                else:
                    curr_val = int(row[0])
                    curr_count = 0
                    for v in row:
                        if int(v) == curr_val:
                            curr_count += 1
                        else:
                            packed.append([curr_count, curr_val])
                            curr_val = int(v)
                            curr_count = 1
                    packed.append([curr_count, curr_val])
                rle_rows.append(packed)
            
            with open(q_p, 'w') as f:
                json.dump(rle_rows, f, separators=(',', ':'))

    return True

def build_climate_grid(era_name, ens_path):
    print(f"Building Climate Grid Chunks for {era_name}...")
    ds = netCDF4.Dataset(ens_path)
    
    t_data = ds.variables['air_temperature'][:]
    np.nan_to_num(t_data, copy=False, nan=0.0)
    
    p_data = ds.variables['precipitation'][:]
    np.nan_to_num(p_data, copy=False, nan=0.0)
    
    ds.close()
    
    H, W = t_data.shape[1], t_data.shape[2]
    out_base = os.path.join(OUT_DIR, era_name, 'climate_grid')
    os.makedirs(out_base, exist_ok=True)
    
    total_steps = 18 * 36
    step = 0
    # Split into 10x10 degree chunks for efficient fetching
    for lat_c in range(-90, 90, 10):
        for lon_c in range(-180, 180, 10):
            y_s, y_e = int((90-(lat_c+10))/180*H), int((90-lat_c)/180*H)
            x_s, x_e = int((lon_c+180)/360*W), int(((lon_c+10)+180)/360*W)
            
            fn = os.path.join(out_base, f"{lat_c}_{lon_c}.json")
            if os.path.exists(fn):
                step += 1
                continue
                
            chunk = {}
            # Speed up: only iterate over land cells in this chunk
            chunk_t = t_data[:, y_s:y_e, x_s:x_e]
            chunk_p = p_data[:, y_s:y_e, x_s:x_e]
            land_mask = np.any(chunk_t != 0, axis=0) | np.any(chunk_p != 0, axis=0)
            
            if np.any(land_mask):
                valid_ys, valid_xs = np.where(land_mask)
                for iy, ix in zip(valid_ys, valid_xs):
                    y_abs, x_abs = y_s + iy, x_s + ix
                    chunk[f"{y_abs}_{x_abs}"] = {
                        "t": [round(float(t_data[m, y_abs, x_abs]), 1) for m in range(12)],
                        "p": [int(round(float(p_data[m, y_abs, x_abs]))) for m in range(12)]
                    }
            
            if chunk:
                with open(fn, 'w') as f:
                    json.dump(chunk, f, separators=(',', ':'))
            
            step += 1
            if step % 50 == 0:
                print(f"  > Climate Grid: {int(step/total_steps*100)}% complete...", flush=True)

    del t_data
    del p_data
    gc.collect()

    return True

if __name__ == "__main__":
    print(f"Memory-Safe Multiprocess Builder (using {MAX_WORKERS} workers)")
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

    # Prepare all tasks
    all_tasks = []
    for era in ERAS:
        ens_path = f'All data/climate_data_0p1/{era}/ensemble_mean_0p1.nc'
        k_path = f'All data/koppen_geiger_nc/{era}/koppen_geiger_0p00833333.nc'
        
        # 1. GENERATE CLIMATE GRID ONCE (Low Res 0.1 deg)
        build_climate_grid(era, ens_path)

        print(f"Adding Tile Tasks for Era: {era}")
        all_tasks.extend([(z, x, y, era, ens_path, k_path) for z in range(Z_MAX+1) for x in range(2**z) for y in range(2**z)])

    print(f"Processing {len(all_tasks)} tile groups...")
    with multiprocessing.Pool(processes=MAX_WORKERS, initializer=init_worker, initargs=(COLORS,)) as pool:
        count = 0
        total_tasks = len(all_tasks)
        for _ in pool.imap_unordered(process_tile_task, all_tasks):
            count += 1
            if count % 1000 == 0:
                print(f"Done: {count}/{total_tasks}", flush=True)

    print("\nCOMPILE COMPLETE!")
