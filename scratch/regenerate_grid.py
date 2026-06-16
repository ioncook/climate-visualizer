import os
import json
import numpy as np
import netCDF4
import multiprocessing
import time

OUT_DIR = 'docs'
ERAS = ["1901_1930", "1931_1960", "1961_1990", "1991_2020"]

def build_climate_grid_era(era):
    start = time.time()
    print(f"Starting {era} (Optimized)...")
    ens_path = f'All data/climate_data_0p1/{era}/ensemble_mean_0p1.nc'
    if not os.path.exists(ens_path):
        print(f"Missing {ens_path}")
        return
    ds = netCDF4.Dataset(ens_path)
    t_data = ds.variables['air_temperature'][:]; np.nan_to_num(t_data, copy=False, nan=0.0)
    p_data = ds.variables['precipitation'][:]; np.nan_to_num(p_data, copy=False, nan=0.0)
    ds.close()
    
    H, W = t_data.shape[1], t_data.shape[2]
    out_base = os.path.join(OUT_DIR, era, 'climate_grid')
    os.makedirs(out_base, exist_ok=True)
    
    written = 0
    for lat_c in range(-90, 90, 10):
        for lon_c in range(-180, 180, 10):
            fn = os.path.join(out_base, f"{lat_c}_{lon_c}.json")
            y_s, y_e = int((90-(lat_c+10))/180*H), int((90-lat_c)/180*H)
            x_s, x_e = int((lon_c+180)/360*W), int(((lon_c+10)+180)/360*W)
            chunk = {}
            chunk_t = t_data[:, y_s:y_e, x_s:x_e]
            chunk_p = p_data[:, y_s:y_e, x_s:x_e]
            land_mask = np.any(chunk_t != 0, axis=0) | np.any(chunk_p != 0, axis=0)
            if np.any(land_mask):
                valid_ys, valid_xs = np.where(land_mask)
                # Fast numpy rounding
                t_rounded = np.round(chunk_t, 1)
                p_rounded = np.round(chunk_p, 1)
                for iy, ix in zip(valid_ys, valid_xs):
                    y_abs, x_abs = y_s + iy, x_s + ix
                    chunk[f"{y_abs}_{x_abs}"] = {
                        "t": t_rounded[:, iy, ix].tolist(),
                        "p": p_rounded[:, iy, ix].tolist()
                    }
            if chunk:
                with open(fn, 'w') as f:
                    json.dump(chunk, f, separators=(',', ':'))
                written += 1
    print(f"Finished {era} in {time.time() - start:.1f}s (wrote {written} chunks)")

def main():
    start = time.time()
    print("Regenerating climate grid JSON chunks in parallel (highly optimized)...")
    with multiprocessing.Pool(processes=len(ERAS)) as pool:
        pool.map(build_climate_grid_era, ERAS)
    print(f"All done in {time.time() - start:.1f}s")

if __name__ == '__main__':
    main()
