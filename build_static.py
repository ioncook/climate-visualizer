import os
import json
import math
import numpy as np
import netCDF4
from PIL import Image
import colorsys
import multiprocessing
import gc
from scipy import ndimage

# --- CONFIG ---
Z_MAX = 7
OUT_DIR = 'docs'
ERAS = ["1901_1930", "1931_1960", "1961_1990", "1991_2020"]
MAX_WORKERS = 4 

# Global caches for workers
_color_lut = None
_precip_lut = None
_temp_lut = None
_era_data = None 

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

    _temp_lut = np.zeros((1001, 4), dtype=np.uint8)
    points = [
        (-70.15, 313.3, 24.3, 36.3), (-55.15, 314.0, 22.1, 73.3),
        (-40.15, 311.1, 39.7, 45.5), (-25.15, 280.5, 31.7, 50.6),
        (-15.15, 178.1, 46.3, 73.7), (-8.15, 172.9, 39.9, 58.2),
        (-4.15, 195.5, 40.3, 56.7), (0.0, 217.1, 47.9, 57.1),
        (0.85, 152.6, 29.5, 37.8), (9.85, 69.3, 71.9, 33.5),
        (20.85, 44.9, 96.8, 48.4), (29.85, 16.8, 81.8, 50.4),
        (46.85, 11.8, 100.0, 13.9)
    ]
    for i in range(1001):
        t = (i / 10.0) - 40
        clamped = max(-70.15, min(46.85, t))
        h, s, l = points[0][1], points[0][2], points[0][3]
        for j in range(len(points)-1):
            if clamped >= points[j][0] and clamped <= points[j+1][0]:
                ratio = (clamped - points[j][0]) / (points[j+1][0] - points[j][0])
                h = points[j][1] + ratio * (points[j+1][1] - points[j][1])
                s = points[j][2] + ratio * (points[j+1][2] - points[j][2])
                l = points[j][3] + ratio * (points[j+1][3] - points[j][3])
                break
        r, g, b = colorsys.hls_to_rgb(h / 360.0, l / 100.0, s / 100.0)
        _temp_lut[i] = [int(r*255), int(g*255), int(b*255), 255]

def fill_missing(data, invalid=None):
    if invalid is None:
        invalid = (data == 0) | np.isnan(data)
    else:
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
        for m in range(t.shape[0]):
            t[m] = fill_missing(t[m], v_t[m].mask if hasattr(v_t, 'mask') else None)
            p[m] = fill_missing(p[m], v_p[m].mask if hasattr(v_p, 'mask') else None)
        ds_e.close()
        ds_k = netCDF4.Dataset(koppen_path_data)
        v_k_name = [v for v in ds_k.variables.keys() if 'kg_class' in v.lower()][0]
        m_k = np.nan_to_num(ds_k.variables[v_k_name][:], 0).astype(np.uint8)
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
    if not os.path.exists(koppen_tile_path):
        tile_classes = master_data[LAT_1KM, LON_1KM]
        img_k = _color_lut[tile_classes]
        img_k[void_mask] = [0, 0, 0, 0]
        if np.any(img_k[:,:,3] > 0):
            Image.fromarray(img_k, 'RGBA').save(koppen_tile_path, compress_level=1)

    # 2. MONTHS
    for m in range(13):
        p_p = os.path.join(era_dir, f'tiles_precip/{m}/{z}/{x}/{y}.png')
        t_p = os.path.join(era_dir, f'tiles_temp/{m}/{z}/{x}/{y}.png')
        
        if not os.path.exists(p_p):
            os.makedirs(os.path.dirname(p_p), exist_ok=True)
            vals_p = mean_precip[LAT_IDX, LON_IDX] if m == 12 else p_data[m, LAT_IDX, LON_IDX]
            img_p = _precip_lut[np.clip(vals_p, 0, 500).astype(int)].copy()
            img_p[water_mask] = img_p[void_mask] = [0, 0, 0, 0]
            if np.any(img_p[:,:,3] > 0):
                Image.fromarray(img_p, 'RGBA').save(p_p, compress_level=1)
        
        if not os.path.exists(t_p):
            os.makedirs(os.path.dirname(t_p), exist_ok=True)
            vals_t = mean_temp[LAT_IDX, LON_IDX] if m == 12 else t_data[m, LAT_IDX, LON_IDX]
            idx = np.clip((vals_t + 40) * 10, 0, 1000).astype(int)
            img_t = _temp_lut[idx].copy()
            img_t[water_mask] = img_t[void_mask] = [0, 0, 0, 0]
            if np.any(img_t[:,:,3] > 0):
                Image.fromarray(img_t, 'RGBA').save(t_p, compress_level=1)
            
    if z == 7:
        q_dir = os.path.join(era_dir, 'koppen_rle', str(z), str(x))
        q_p = os.path.join(q_dir, f"{y}.json")
        if not os.path.exists(q_p):
            os.makedirs(q_dir, exist_ok=True)
            tile_classes = master_data[LAT_1KM, LON_1KM]
            rle_rows = []
            for row in tile_classes:
                packed = []
                if np.all(row == row[0]): packed = [[256, int(row[0])]]
                else:
                    curr_val = int(row[0]); curr_count = 0
                    for v in row:
                        if int(v) == curr_val: curr_count += 1
                        else: packed.append([curr_count, curr_val]); curr_val = int(v); curr_count = 1
                    packed.append([curr_count, curr_val])
                rle_rows.append(packed)
            with open(q_p, 'w') as f: json.dump(rle_rows, f, separators=(',', ':'))
    return True

def build_climate_grid(era_name, ens_path):
    print(f"Checking Climate Grid Chunks for {era_name}...")
    ds = netCDF4.Dataset(ens_path)
    t_data = ds.variables['air_temperature'][:]; np.nan_to_num(t_data, copy=False, nan=0.0)
    p_data = ds.variables['precipitation'][:]; np.nan_to_num(p_data, copy=False, nan=0.0)
    ds.close()
    H, W = t_data.shape[1], t_data.shape[2]
    out_base = os.path.join(OUT_DIR, era_name, 'climate_grid')
    os.makedirs(out_base, exist_ok=True)
    for lat_c in range(-90, 90, 10):
        for lon_c in range(-180, 180, 10):
            fn = os.path.join(out_base, f"{lat_c}_{lon_c}.json")
            if os.path.exists(fn): continue
            y_s, y_e = int((90-(lat_c+10))/180*H), int((90-lat_c)/180*H)
            x_s, x_e = int((lon_c+180)/360*W), int(((lon_c+10)+180)/360*W)
            chunk = {}
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
                with open(fn, 'w') as f: json.dump(chunk, f, separators=(',', ':'))
    del t_data; del p_data; gc.collect()
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
    all_tasks = []
    for era in ERAS:
        ens_path = f'All data/climate_data_0p1/{era}/ensemble_mean_0p1.nc'
        k_path = f'All data/koppen_geiger_nc/{era}/koppen_geiger_0p00833333.nc'
        build_climate_grid(era, ens_path)
        print(f"Scanning tasks for Era: {era}...")
        era_tasks = []
        for z in range(Z_MAX+1):
            for x in range(2**z):
                for y in range(2**z):
                    # PRE-FILTER: Only add tasks that don't have finished tiles
                    last_tile = os.path.join(OUT_DIR, era, f'tiles_temp/12/{z}/{x}/{y}.png')
                    koppen_tile = os.path.join(OUT_DIR, era, f'tiles_koppen/{z}/{x}/{y}.png')
                    if not (os.path.exists(last_tile) and os.path.exists(koppen_tile)):
                        era_tasks.append((z, x, y, era, ens_path, k_path))
        print(f"  > Added {len(era_tasks)} missing tasks.")
        all_tasks.extend(era_tasks)
    
    if not all_tasks:
        print("\nALL TILES ARE ALREADY GENERATED!")
    else:
        print(f"\nStarting generation for {len(all_tasks)} missing tiles...")
        with multiprocessing.Pool(processes=MAX_WORKERS, initializer=init_worker, initargs=(COLORS,)) as pool:
            count = 0
            total_tasks = len(all_tasks)
            for _ in pool.imap_unordered(process_tile_task, all_tasks, chunksize=5):
                count += 1
                if count % 100 == 0: print(f"Done: {count}/{total_tasks}", flush=True)
    print("\nBUILD COMPLETE!")
