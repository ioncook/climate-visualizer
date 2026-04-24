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

# Config
Z_MAX = 7
OUT_DIR = 'docs'
ERAS = ["1901_1930", "1931_1960", "1961_1990", "1991_2020"]
MAX_WORKERS = 4

_era_data_cache = {}
_k_lut = None

def fill_missing(data, invalid=None):
    if invalid is None:
        invalid = (data == 0) | np.isnan(data)
    else:
        invalid = invalid | (data == 0)
    if not np.any(invalid): return data
    ind = ndimage.distance_transform_edt(invalid, return_distances=False, return_indices=True)
    return data[tuple(ind)]

def _init_luts():
    global _k_lut
    _k_lut = np.zeros((61, 4), dtype=np.uint8)
    lut = {
        0: [0, 255, 0, 255], 1: [0, 255, 255, 255], 2: [0, 0, 255, 255], 3: [128, 0, 255, 255],
        -1: [255, 255, 0, 255], -2: [255, 128, 0, 255], -3: [255, 0, 0, 255]
    }
    for diff in range(-30, 31):
        idx = diff + 30
        d = max(-3, min(3, diff))
        _k_lut[idx] = lut.get(d, lut[0])

def _hsl_array_to_rgba(H_deg, void_mask, water_mask):
    H = H_deg / 360.0
    q = np.full_like(H, 1.0)
    p = np.zeros_like(H)
    def htr(t):
        t = np.where(t < 0.0, t + 1.0, t)
        t = np.where(t > 1.0, t - 1.0, t)
        return np.select([t < 1/6.0, t < 1/2.0, t < 2/3.0], [p+(q-p)*6.0*t, q, p+(q-p)*(2/3.0-t)*6.0], default=p)
    R, G, B = htr(H+1/3.0), htr(H), htr(H-1/3.0)
    arr = np.stack([(R*255).astype(np.uint8), (G*255).astype(np.uint8), (B*255).astype(np.uint8), np.full(H.shape, 255, dtype=np.uint8)], axis=-1)
    arr[void_mask | water_mask] = [0, 0, 0, 0]
    return arr

def get_temp_color_array(t1, t2, void_mask, water_mask):
    diff_f = (t2 - t1) * 1.8
    pos_n, neg_n = np.clip(diff_f / 3.0, 0, 1), np.clip(np.abs(diff_f) / 1.5, 0, 1)
    hue = np.where(diff_f >= 0, 120.0 * (1.0 - pos_n), 120.0 + 120.0 * neg_n)
    return _hsl_array_to_rgba(hue, void_mask, water_mask)

def get_precip_color_array(p1, p2, void_mask, water_mask):
    denom = np.maximum(np.abs(p1), 1.0)
    diff_pct = (p2 - p1) / denom * 100.0
    mag = np.log10(np.abs(diff_pct) + 1.0)
    norm_mag = np.clip(mag / 2.0, 0.0, 1.0)
    hue = np.where(diff_pct > 0, 120.0 + 120.0 * norm_mag, 120.0 * (1.0 - norm_mag))
    hue[np.abs(diff_pct) < 1.0] = 120.0
    return _hsl_array_to_rgba(hue, void_mask, water_mask)

def _gen_arrays(z, x, y):
    num_tiles = 2 ** z
    map_size = 256 * num_tiles
    px, py = x * 256 + np.arange(256), y * 256 + np.arange(256)
    lons = 360 * ((px / map_size) - 0.5)
    y_norm = 0.5 - (py / map_size)
    lats = 90 - 360 * np.arctan(np.exp(-y_norm * 2 * np.pi)) / np.pi
    return lons, lats

def get_era_data(era_name):
    global _era_data_cache
    if era_name not in _era_data_cache:
        print(f"[{os.getpid()}] Worker loading data for {era_name}...")
        ds = netCDF4.Dataset(f'All data/climate_data_0p1/{era_name}/ensemble_mean_0p1.nc')
        ds_k = netCDF4.Dataset(f'All data/koppen_geiger_nc/{era_name}/koppen_geiger_0p00833333.nc')
        v_k = [v for v in ds_k.variables.keys() if 'kg_class' in v.lower()][0]
        
        # Only compute the mean directly, we don't need monthly data for comparisons
        t_mean = np.mean(ds.variables['air_temperature'][:], axis=0)
        p_mean = np.mean(ds.variables['precipitation'][:], axis=0)
        
        np.nan_to_num(t_mean, copy=False, nan=0.0)
        np.nan_to_num(p_mean, copy=False, nan=0.0)
        
        # Run the expensive nearest-neighbor extrapolation only ONCE on the 2D mean
        t_mean = fill_missing(t_mean)
        p_mean = fill_missing(p_mean)
        
        k = np.nan_to_num(ds_k.variables[v_k][:], 0).astype(np.int8)
        _era_data_cache[era_name] = {'mT': t_mean, 'mP': p_mean, 'k': k, 'shape': t_mean.shape, 'mshape': k.shape}
        ds.close(); ds_k.close(); gc.collect()
        print(f"[{os.getpid()}] Worker finished loading {era_name}.")
    return _era_data_cache[era_name]

def process_compare_task(args):
    z, x, y, era1, era2 = args
    d1, d2 = get_era_data(era1), get_era_data(era2)
    comp_dir = os.path.join(OUT_DIR, 'compare', f'{era1}_{era2}')
    koppen_path, precip_path, temp_path = os.path.join(comp_dir, f'tiles_koppen/{z}/{x}/{y}.png'), os.path.join(comp_dir, f'tiles_precip/12/{z}/{x}/{y}.png'), os.path.join(comp_dir, f'tiles_temp/12/{z}/{x}/{y}.png')
    
    lons, lats = _gen_arrays(z, x, y)
    E_H, E_W = d1['shape']; H, W = d1['mshape']
    lon_idx, lon_1km = (np.floor((lons + 180) / 360 * E_W).astype(int) % E_W), (np.floor((lons + 180) / 360 * W).astype(int) % W)
    lat_idx_raw, lat_1km_raw = np.floor((90 - lats) / 180 * E_H).astype(int), np.floor((90 - lats) / 180 * H).astype(int)
    lat_idx, lat_1km = np.clip(lat_idx_raw, 0, E_H - 1), np.clip(lat_1km_raw, 0, H - 1)
    LON_IDX, LAT_IDX = np.meshgrid(lon_idx, lat_idx)
    LON_1KM, LAT_1KM = np.meshgrid(lon_1km, lat_1km)
    water_mask, void_mask = d1['k'][LAT_1KM, LON_1KM] == 0, np.logical_not((lat_idx_raw >= 0) & (lat_idx_raw < E_H))
    _, VOID_GRID = np.meshgrid(lon_idx, void_mask)

    if not os.path.exists(koppen_path):
        os.makedirs(os.path.dirname(koppen_path), exist_ok=True)
        diff_k = np.clip(d2['k'][LAT_1KM, LON_1KM].astype(int) - d1['k'][LAT_1KM, LON_1KM].astype(int), -30, 30) + 30
        img_k = _k_lut[diff_k].copy(); img_k[VOID_GRID | water_mask] = [0, 0, 0, 0]
        if np.any(img_k[:,:,3] > 0): Image.fromarray(img_k, 'RGBA').save(koppen_path)
    if not os.path.exists(precip_path):
        os.makedirs(os.path.dirname(precip_path), exist_ok=True)
        img_p = get_precip_color_array(d1['mP'][LAT_IDX, LON_IDX], d2['mP'][LAT_IDX, LON_IDX], VOID_GRID, water_mask)
        if np.any(img_p[:,:,3] > 0): Image.fromarray(img_p, 'RGBA').save(precip_path)
    if not os.path.exists(temp_path):
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        img_t = get_temp_color_array(d1['mT'][LAT_IDX, LON_IDX], d2['mT'][LAT_IDX, LON_IDX], VOID_GRID, water_mask)
        if np.any(img_t[:,:,3] > 0): Image.fromarray(img_t, 'RGBA').save(temp_path)
    return True

def _init_worker():
    _init_luts()

if __name__ == "__main__":
    print(f"Comparison Builder (using {MAX_WORKERS} workers)")
    all_tasks = []
    for i in range(len(ERAS)):
        for j in range(i + 1, len(ERAS)):
            era1, era2 = ERAS[i], ERAS[j]
            print(f"Scanning for {era1} vs {era2}...")
            pair_tasks = []
            for z in range(Z_MAX + 1):
                for x in range(2**z):
                    for y in range(2**z):
                        # PRE-FILTER
                        t_path = os.path.join(OUT_DIR, 'compare', f'{era1}_{era2}', f'tiles_temp/12/{z}/{x}/{y}.png')
                        if not os.path.exists(t_path):
                            pair_tasks.append((z, x, y, era1, era2))
            print(f"  > Added {len(pair_tasks)} missing tasks.")
            all_tasks.extend(pair_tasks)

    if not all_tasks:
        print("\nALL COMPARISON TILES ARE ALREADY GENERATED!")
    else:
        print(f"\nStarting comparison for {len(all_tasks)} missing tiles...")
        with multiprocessing.Pool(processes=MAX_WORKERS, initializer=_init_worker) as pool:
            count = 0
            total = len(all_tasks)
            for _ in pool.imap_unordered(process_compare_task, all_tasks, chunksize=10):
                count += 1
                if count % 100 == 0: print(f"Done: {count}/{total}", flush=True)
    print("\nALL COMPARISONS DONE")
