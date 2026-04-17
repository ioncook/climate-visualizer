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

# Config
Z_MAX = 7
OUT_DIR = 'docs'
ERAS = ["1901_1930", "1931_1960", "1961_1990", "1991_2020"]
MAX_WORKERS = 1

_era_data_cache = {}
_k_lut = None

def fill_missing(data, invalid=None):
    # Fill zeros/NaNs using nearest neighbor extrapolation from the nearest valid data points
    if invalid is None:
        invalid = (data == 0) | np.isnan(data)
    else:
        # Augment the mask with zero-check to catch shoreline artifacts
        invalid = invalid | (data == 0)
        
    if not np.any(invalid): return data
    ind = ndimage.distance_transform_edt(invalid, return_distances=False, return_indices=True)
    return data[tuple(ind)]

# Köppen diff mapping (ROYGBIV)
# We want: 
# Increase in legend index (typically colder): Cool colors (-1 Cyan to -3 Violet)
# Decrease in legend index (typically warmer): Warm colors (+1 Yellow to +3 Red)
def _init_luts():
    global _k_lut
    _k_lut = np.zeros((61, 4), dtype=np.uint8)  # max diff is +-30 -> offset by 30
    lut = {
        0: [0, 255, 0, 255],      # green
        -1: [255, 255, 0, 255],   # yellow
        -2: [255, 128, 0, 255],   # orange
        -3: [255, 0, 0, 255],     # red
        1: [0, 255, 255, 255],    # cyan
        2: [0, 0, 255, 255],      # blue
        3: [128, 0, 255, 255]     # violet
    }
    for diff in range(-30, 31):
        idx = diff + 30
        if diff <= -3:
            _k_lut[idx] = lut[-3]
        elif diff >= 3:
            _k_lut[idx] = lut[3]
        else:
            _k_lut[idx] = lut.get(diff, lut[0])

def _init_worker():
    _init_luts()

def _hsl_array_to_rgba(H_deg, void_mask, water_mask):
    """Convert arrays of hue (degrees), with fixed L=0.5 S=1.0, to RGBA."""
    H = H_deg / 360.0
    q = np.full_like(H, 1.0)  # L=0.5, S=1 => q = L+S-L*S = 1.0
    p = np.zeros_like(H)       # 2*L - q = 0.0

    def htr(t):
        t = np.where(t < 0.0, t + 1.0, t)
        t = np.where(t > 1.0, t - 1.0, t)
        return np.select(
            [t < 1/6.0, t < 1/2.0, t < 2/3.0],
            [p + (q - p) * 6.0 * t, q, p + (q - p) * (2/3.0 - t) * 6.0],
            default=p
        )

    R_f = htr(H + 1/3.0)
    G_f = htr(H)
    B_f = htr(H - 1/3.0)

    arr = np.stack([
        (R_f * 255).astype(np.uint8),
        (G_f * 255).astype(np.uint8),
        (B_f * 255).astype(np.uint8),
        np.full(H.shape, 255, dtype=np.uint8)
    ], axis=-1)
    arr[void_mask] = [0, 0, 0, 0]
    arr[water_mask] = [0, 0, 0, 0]
    return arr

def _pct_diff_magnitude(v1, v2):
    """Gentler log-scaled normalised magnitude [0..1]. Saturates around 300% change."""
    denom = np.maximum(np.abs(v1), 1.0)
    diff_pct = (v2 - v1) / denom * 100.0
    mag = np.log10(np.abs(diff_pct) + 1.0)
    norm_mag = np.clip(mag / 2.0, 0.0, 1.0)  # /2 => saturates at ~100x (softer curve)
    return diff_pct, norm_mag

def get_temp_color_array(t1, t2, void_mask, water_mask):
    """Temp change: Absolute degF. Blue=-1.5F, Green=0, Red=+3.0F."""
    diff_c = t2 - t1
    diff_f = diff_c * 1.8  # Convert Celsius delta to Fahrenheit delta
    
    # For positive side (0 up to +3.0 F)
    pos_n = np.clip(diff_f / 3.0, 0, 1)
    # For negative side (-1.5 F up to 0)
    neg_n = np.clip(np.abs(diff_f) / 1.5, 0, 1)

    # Hue: Red=0, Green=120, Blue=240
    hue = np.where(diff_f >= 0, 120.0 * (1.0 - pos_n), 120.0 + 120.0 * neg_n)
    
    return _hsl_array_to_rgba(hue, void_mask, water_mask)

def get_precip_color_array(p1, p2, void_mask, water_mask):
    """Precip change: positive=blue(240°), negative=red(0°), zero=green(120°)."""
    diff_pct, norm_mag = _pct_diff_magnitude(p1, p2)
    hue = np.where(diff_pct > 0, 120.0 + 120.0 * norm_mag, 120.0 * (1.0 - norm_mag))
    no_change = np.abs(diff_pct) < 1.0
    hue[no_change] = 120.0
    return _hsl_array_to_rgba(hue, void_mask, water_mask)

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

def get_era_data(era_name):
    global _era_data_cache
    if era_name not in _era_data_cache:
        ds = netCDF4.Dataset(f'All data/climate_data_0p1/{era_name}/ensemble_mean_0p1.nc')
        ds_k = netCDF4.Dataset(f'All data/koppen_geiger_nc/{era_name}/koppen_geiger_0p00833333.nc')
        v_k = [v for v in ds_k.variables.keys() if 'kg_class' in v.lower()][0]
        
        v_t = ds.variables['air_temperature']
        v_p = ds.variables['precipitation']
        t = v_t[:]
        np.nan_to_num(t, copy=False, nan=0.0)
        p = v_p[:]
        np.nan_to_num(p, copy=False, nan=0.0)

        # Extrapolate data into ocean for cleaner masking at the coastline
        for i in range(12):
            t[i] = fill_missing(t[i], v_t[i].mask if hasattr(v_t, 'mask') else None)
            p[i] = fill_missing(p[i], v_p[i].mask if hasattr(v_p, 'mask') else None)

        k = np.nan_to_num(ds_k.variables[v_k][:], 0).astype(np.int8)
        
        _era_data_cache[era_name] = {
            'mT': np.mean(t, axis=0), 
            'mP': np.mean(p, axis=0), 
            'k': k, 
            'shape': (t.shape[1], t.shape[2]), 
            'mshape': k.shape
        }
        ds.close()
        ds_k.close()
        gc.collect()
    return _era_data_cache[era_name]

def process_compare_task(args):
    z, x, y, era1, era2 = args
    
    d1 = get_era_data(era1)
    d2 = get_era_data(era2)

    comp_dir = os.path.join(OUT_DIR, 'compare', f'{era1}_{era2}')
    koppen_path = os.path.join(comp_dir, f'tiles_koppen/{z}/{x}/{y}.png')
    precip_path = os.path.join(comp_dir, f'tiles_precip/12/{z}/{x}/{y}.png')
    temp_path = os.path.join(comp_dir, f'tiles_temp/12/{z}/{x}/{y}.png')

    if os.path.exists(koppen_path) and os.path.exists(precip_path) and os.path.exists(temp_path):
        return True

    lons, lats = _gen_arrays(z, x, y)
    E_H, E_W = d1['shape']
    H, W = d1['mshape']
    
    lon_idx = np.floor((lons + 180) / 360 * E_W).astype(int) % E_W
    lon_1km = np.floor((lons + 180) / 360 * W).astype(int) % W
    lat_idx_raw = np.floor((90 - lats) / 180 * E_H).astype(int)
    lat_1km_raw = np.floor((90 - lats) / 180 * H).astype(int)
    lat_idx, lat_1km = np.clip(lat_idx_raw, 0, E_H - 1), np.clip(lat_1km_raw, 0, H - 1)
    
    LON_IDX, LAT_IDX = np.meshgrid(lon_idx, lat_idx)
    LON_1KM, LAT_1KM = np.meshgrid(lon_1km, lat_1km)
    LAT_VALID = (lat_idx_raw >= 0) & (lat_idx_raw < E_H)
    _, LAT_VALID_GRID = np.meshgrid(lon_idx, LAT_VALID)
    
    water_mask = d1['k'][LAT_1KM, LON_1KM] == 0
    void_mask = np.logical_not(LAT_VALID_GRID)

    os.makedirs(os.path.dirname(koppen_path), exist_ok=True)
    os.makedirs(os.path.dirname(precip_path), exist_ok=True)
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)

    # 1. KOPPEN
    if not os.path.exists(koppen_path):
        k1_c = d1['k'][LAT_1KM, LON_1KM].astype(int)
        k2_c = d2['k'][LAT_1KM, LON_1KM].astype(int)
        diff_k = np.clip(k2_c - k1_c, -30, 30) + 30
        img_k = _k_lut[diff_k].copy()
        img_k[void_mask] = [0, 0, 0, 0]
        img_k[water_mask] = [0, 0, 0, 0]
        if np.any(img_k[:,:,3] > 0):
            Image.fromarray(img_k, 'RGBA').save(koppen_path)

    # 2. PRECIP (positive=blue, negative=red)
    if not os.path.exists(precip_path):
        p1_c = d1['mP'][LAT_IDX, LON_IDX]
        p2_c = d2['mP'][LAT_IDX, LON_IDX]
        img_p = get_precip_color_array(p1_c, p2_c, void_mask, water_mask)
        if np.any(img_p[:,:,3] > 0):
            Image.fromarray(img_p, 'RGBA').save(precip_path)

    # 3. TEMP (positive=red, negative=blue)
    if not os.path.exists(temp_path):
        t1_c = d1['mT'][LAT_IDX, LON_IDX]
        t2_c = d2['mT'][LAT_IDX, LON_IDX]
        img_t = get_temp_color_array(t1_c, t2_c, void_mask, water_mask)
        if np.any(img_t[:,:,3] > 0):
            Image.fromarray(img_t, 'RGBA').save(temp_path)

    return True

if __name__ == "__main__":
    print(f"Building Compare Tiles with {MAX_WORKERS} workers")
    all_tasks = []
    for i in range(len(ERAS)):
        for j in range(i + 1, len(ERAS)):
            era1 = ERAS[i]
            era2 = ERAS[j]
            comp_dir = os.path.join(OUT_DIR, 'compare', f'{era1}_{era2}')
            os.makedirs(comp_dir, exist_ok=True)
            for z in range(Z_MAX + 1):
                for x in range(2**z):
                    for y in range(2**z):
                        all_tasks.append((z, x, y, era1, era2))
    
    def _init_worker():
        _init_luts()

    with multiprocessing.Pool(processes=MAX_WORKERS, initializer=_init_worker) as pool:
        count = 0
        total_tasks = len(all_tasks)
        for _ in pool.imap_unordered(process_compare_task, all_tasks):
            count += 1
            if count % 1000 == 0:
                print(f"Done: {count}/{total_tasks}", flush=True)
                # Suggest GC periodically if needed, though Pool workers handle their own memory
    
    print("\nALL COMPARISONS DONE")
