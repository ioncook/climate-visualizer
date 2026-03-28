import os
import numpy as np
import netCDF4
from PIL import Image
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc
from scipy import ndimage

# Config
Z_MAX = 6
OUT_DIR = 'docs'
ERA = "1991_2020"
MAX_WORKERS = 2

_era_data = None
_color_lut = None

def _init_lut():
    global _color_lut
    # Jan (0) -> Magenta/Purple (320), Dec (11) -> Vivid Red (0)
    # This range (320 -> 0) means Jan and Dec are only 40 deg apart (both reddish)
    _color_lut = np.zeros((12, 4), dtype=np.uint8)
    for m in range(12):
        hue = 320 * (1 - m/11.0)
        # Convert HSL(hue, 100%, 50%) to RGB
        c = 1.0 # chroma
        x = c * (1 - abs((hue / 60.0) % 2 - 1))
        if 0 <= hue < 60: r,g,b = c,x,0
        elif 60 <= hue < 120: r,g,b = x,c,0
        elif 120 <= hue < 180: r,g,b = 0,c,x
        elif 180 <= hue < 240: r,g,b = 0,x,c
        elif 240 <= hue < 300: r,g,b = x,0,c
        else: r,g,b = c,0,x
        
        _color_lut[m] = [int(r*255), int(g*255), int(b*255), 255]

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

def _init_worker():
    _init_lut()

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

def process_extreme_task(args):
    z, x, y = args
    global _era_data
    
    if _era_data is None:
        ens_path = f'All data/climate_data_0p1/{ERA}/ensemble_mean_0p1.nc'
        k_path = f'All data/koppen_geiger_nc/{ERA}/koppen_geiger_0p00833333.nc'
        
        ds = netCDF4.Dataset(ens_path)
        v_p = ds.variables['precipitation']
        p = np.nan_to_num(v_p[:12], 0) # Only monthly
        for m in range(p.shape[0]):
            p[m] = fill_missing(p[m], v_p[m].mask if hasattr(v_p, 'mask') else None)
        ds.close()
        
        ds_k = netCDF4.Dataset(k_path)
        v_k = [v for v in ds_k.variables.keys() if 'kg_class' in v.lower()][0]
        m_k = np.nan_to_num(ds_k.variables[v_k][:], 0).astype(np.uint8)
        ds_k.close()
        
        _era_data = {
            'max_idx': np.argmax(p, axis=0),
            'min_idx': np.argmin(p, axis=0),
            'k': m_k,
            'shape': (p.shape[1], p.shape[2]),
            'm_shape': m_k.shape
        }
        gc.collect()

    era_dir = os.path.join(OUT_DIR, ERA)
    max_path = os.path.join(era_dir, f'tiles_precip_max/{z}/{x}/{y}.png')
    min_path = os.path.join(era_dir, f'tiles_precip_min/{z}/{x}/{y}.png')

    if os.path.exists(max_path) and os.path.exists(min_path):
        return True

    lons, lats = _gen_arrays(z, x, y)
    E_H, E_W = _era_data['shape']
    H, W = _era_data['m_shape']
    
    lon_idx = np.floor((lons + 180) / 360 * E_W).astype(int) % E_W
    lon_1km = np.floor((lons + 180) / 360 * W).astype(int) % W
    lat_idx_raw = np.floor((90 - lats) / 180 * E_H).astype(int)
    lat_1km_raw = np.floor((90 - lats) / 180 * H).astype(int)
    lat_idx, lat_1km = np.clip(lat_idx_raw, 0, E_H - 1), np.clip(lat_1km_raw, 0, H - 1)
    
    LON_IDX, LAT_IDX = np.meshgrid(lon_idx, lat_idx)
    LON_1KM, LAT_1KM = np.meshgrid(lon_1km, lat_1km)
    LAT_VALID = (lat_idx_raw >= 0) & (lat_idx_raw < E_H)
    _, LAT_VALID_GRID = np.meshgrid(lon_idx, LAT_VALID)
    
    water_mask = _era_data['k'][LAT_1KM, LON_1KM] == 0
    void_mask = np.logical_not(LAT_VALID_GRID)

    os.makedirs(os.path.dirname(max_path), exist_ok=True)
    os.makedirs(os.path.dirname(min_path), exist_ok=True)

    # 1. MAX
    if not os.path.exists(max_path):
        m_idx = _era_data['max_idx'][LAT_IDX, LON_IDX]
        img = _color_lut[m_idx].copy()
        img[water_mask] = img[void_mask] = [0, 0, 0, 0]
        Image.fromarray(img, 'RGBA').save(max_path)

    # 2. MIN
    if not os.path.exists(min_path):
        m_idx = _era_data['min_idx'][LAT_IDX, LON_IDX]
        img = _color_lut[m_idx].copy()
        img[water_mask] = img[void_mask] = [0, 0, 0, 0]
        Image.fromarray(img, 'RGBA').save(min_path)

    return True

if __name__ == "__main__":
    print(f"Building Precip Extremes for {ERA}")
    all_tasks = [(z, x, y) for z in range(Z_MAX+1) for x in range(2**z) for y in range(2**z)]
    
    with ProcessPoolExecutor(max_workers=MAX_WORKERS, initializer=_init_worker) as executor:
        futures = {executor.submit(process_extreme_task, t): t for t in all_tasks}
        count = 0
        for f in as_completed(futures):
            count += 1
            if count % 1000 == 0:
                print(f"Done: {count}/{len(all_tasks)}")
    
    print("EXTREMES DONE")
