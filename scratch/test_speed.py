import time
import os
import json
import numpy as np
import netCDF4

def test_speed():
    start = time.time()
    era = "1991_2020"
    ens_path = f'All data/climate_data_0p1/{era}/ensemble_mean_0p1.nc'
    ds = netCDF4.Dataset(ens_path)
    t_data = ds.variables['air_temperature'][:]; np.nan_to_num(t_data, copy=False, nan=0.0)
    p_data = ds.variables['precipitation'][:]; np.nan_to_num(p_data, copy=False, nan=0.0)
    ds.close()
    H, W = t_data.shape[1], t_data.shape[2]
    print(f"Loaded netCDF in {time.time() - start:.2f}s")
    
    # Test writing just 5 chunks
    count = 0
    for lat_c in range(0, 30, 10):
        for lon_c in range(0, 30, 10):
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
                        "p": [round(float(p_data[m, y_abs, x_abs]), 1) for m in range(12)]
                    }
            count += 1
            if count >= 5:
                break
        if count >= 5:
            break
    print(f"Processed 5 chunks in {time.time() - start:.2f}s total")

if __name__ == '__main__':
    test_speed()
