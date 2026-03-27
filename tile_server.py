import os
import io
import math
import numpy as np
import netCDF4
from PIL import Image
from http.server import SimpleHTTPRequestHandler, HTTPServer
import threading

# Configuration
PORT = 8001
NC_FILE = 'Current data/koppen_geiger_nc/1991_2020/koppen_geiger_0p00833333.nc'

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

# LUT
color_lut = np.zeros((32, 4), dtype=np.uint8)
for k, v in COLORS.items():
    color_lut[k] = [v[0], v[1], v[2], 200]

# Legends from legend.txt
LEGENDS = {
    1: ["Af", "Tropical rainforest"], 2: ["Am", "Tropical monsoon"], 3: ["Aw", "Tropical savannah"],
    4: ["BWh", "Arid desert, hot"], 5: ["BWk", "Arid desert, cold"], 6: ["BSh", "Arid steppe, hot"], 7: ["BSk", "Arid steppe, cold"],
    8: ["Csa", "Temperate, dry summer, hot summer"], 9: ["Csb", "Temperate, dry summer, warm summer"], 10: ["Csc", "Temperate, dry summer, cold summer"],
    11: ["Cwa", "Temperate, dry winter, hot summer"], 12: ["Cwb", "Temperate, dry winter, warm summer"], 13: ["Cwc", "Temperate, dry winter, cold summer"],
    14: ["Cfa", "Temperate, no dry season, hot summer"], 15: ["Cfb", "Temperate, no dry season, warm summer"], 16: ["Cfc", "Temperate, no dry season, cold summer"],
    17: ["Dsa", "Cold, dry summer, hot summer"], 18: ["Dsb", "Cold, dry summer, warm summer"], 19: ["Dsc", "Cold, dry summer, cold summer"], 20: ["Dsd", "Cold, dry summer, very cold winter"],
    21: ["Dwa", "Cold, dry winter, hot summer"], 22: ["Dwb", "Cold, dry winter, warm summer"], 23: ["Dwc", "Cold, dry winter, cold summer"], 24: ["Dwd", "Cold, dry winter, very cold winter"],
    25: ["Dfa", "Cold, no dry season, hot summer"], 26: ["Dfb", "Cold, no dry season, warm summer"], 27: ["Dfc", "Cold, no dry season, cold summer"], 28: ["Dfd", "Cold, no dry season, very cold winter"],
    29: ["ET", "Polar tundra"], 30: ["EF", "Polar frost"]
}

print("Initializing 1km Tile Server...")
print("Loading Master NetCDF Dataset (39MB)...")
ds = netCDF4.Dataset(NC_FILE)
var_name = [v for v in ds.variables.keys() if 'kg_class' in v.lower()][0]
master_data = np.nan_to_num(ds.variables[var_name][:], 0).astype(np.uint8)
ds.close()

print("Loading Ensemble Mean Data (56MB)...")
ens_file = 'Current data/climate_data_0p1/1991_2020/ensemble_mean_0p1.nc'
ds_ens = netCDF4.Dataset(ens_file)
# (12, 1800, 3600)
temp_data = np.nan_to_num(ds_ens.variables['air_temperature'][:], 0)
precip_data = np.nan_to_num(ds_ens.variables['precipitation'][:], 0)
ds_ens.close()

import colorsys

H, W = master_data.shape
E_H, E_W = temp_data.shape[1], temp_data.shape[2]
print(f"Datasets Loaded. Ready to serve tiles and queries.")

# Precipitation Pre-calculations
print("Pre-calculating Precipitation Data...")
mean_precip = np.mean(precip_data, axis=0)
precip_lut = np.zeros((501, 4), dtype=np.uint8)
for p in range(501):
    curve = (p / 500) ** 0.7
    # 270 degrees out of 360 is 0.75
    hue = (270 * curve) / 360.0 
    r, g, b = colorsys.hls_to_rgb(hue, 0.65, 1.0)
    precip_lut[p] = [int(r*255), int(g*255), int(b*255), 200]

# Cache for fast panning
tile_cache = {}
cache_lock = threading.Lock()

class TileRequestHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        import json
        from urllib.parse import urlparse, parse_qs

        parsed = urlparse(self.path)
        
        # New Query Endpoint
        if parsed.path == '/query':
            params = parse_qs(parsed.query)
            try:
                lat = float(params.get('lat', [0])[0])
                lon = float(params.get('lon', [0])[0])
                
                # 1. Look up Köppen Class (1km Res)
                # lon -180...180 maps to 0...W
                x_idx = int((lon + 180) / 360 * W) % W
                y_idx = int((90 - lat) / 180 * H)
                y_idx = min(max(y_idx, 0), H - 1)
                
                class_id = int(master_data[y_idx, x_idx])
                class_info = LEGENDS.get(class_id, ["Unknown", "No Data"])
                
                # 2. Look up Climate Data (0.1 deg Res)
                ex_idx = int((lon + 180) / 360 * E_W) % E_W
                ey_idx = int((90 - lat) / 180 * E_H)
                ey_idx = min(max(ey_idx, 0), E_H - 1)
                
                monthly_temp = temp_data[:, ey_idx, ex_idx].tolist()
                monthly_precip = precip_data[:, ey_idx, ex_idx].tolist()
                
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                
                result = {
                    "lat": lat, "lon": lon,
                    "id": class_id,
                    "code": class_info[0],
                    "name": class_info[1],
                    "temp": [round(t, 1) for t in monthly_temp],
                    "precip": [round(p, 1) for p in monthly_precip]
                }
                self.wfile.write(json.dumps(result).encode())
                return
            except Exception as e:
                self.send_error(500, str(e))
                return

        # New Precipitation Tile Endpoint
        if self.path.startswith('/dynamic_tiles_precip/'):
            parts = self.path.strip('/').split('/')
            if len(parts) == 4 and parts[-1].endswith('.png'):
                z = int(parts[1])
                x = int(parts[2])
                y = int(parts[3].replace('.png', ''))
                
                cache_key = f"precip_{z}_{x}_{y}"
                with cache_lock:
                    if cache_key in tile_cache:
                        self.send_png(tile_cache[cache_key])
                        return

                png_bytes = self.generate_precip_tile(z, x, y)
                with cache_lock:
                    if len(tile_cache) > 2000: tile_cache.clear()
                    tile_cache[cache_key] = png_bytes
                
                self.send_png(png_bytes)
                return

        # Intercept tile requests: /dynamic_tiles/{z}/{x}/{y}.png
        if self.path.startswith('/dynamic_tiles/'):
            parts = self.path.strip('/').split('/')
            if len(parts) == 4 and parts[-1].endswith('.png'):
                z = int(parts[1])
                x = int(parts[2])
                y = int(parts[3].replace('.png', ''))
                
                # Check cache
                cache_key = f"koppen_{z}_{x}_{y}"
                with cache_lock:
                    if cache_key in tile_cache:
                        self.send_png(tile_cache[cache_key])
                        return

                png_bytes = self.generate_tile(z, x, y)
                
                with cache_lock:
                    if len(tile_cache) > 2000:
                        tile_cache.clear() # Prevent RAM blowout
                    tile_cache[cache_key] = png_bytes
                
                self.send_png(png_bytes)
                return
                
        # Fallback to normal static file serving
        return super().do_GET()

    def send_png(self, img_bytes):
        self.send_response(200)
        self.send_header("Content-Type", "image/png")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(img_bytes)))
        # Tell browser to cache this tile
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()
        self.wfile.write(img_bytes)        

    def generate_precip_tile(self, z, x, y):
        num_tiles = 2 ** z
        map_size = 256 * num_tiles

        px = x * 256 + np.arange(256)
        x_norm = (px / map_size) - 0.5
        lons = 360 * x_norm
        
        py = y * 256 + np.arange(256)
        y_norm = 0.5 - (py / map_size)
        lats = 90 - 360 * np.arctan(np.exp(-y_norm * 2 * np.pi)) / np.pi
        
        # Geographic Longitude MUST wrap identically as zooming out repeats the world
        lon_idx = np.floor((lons + 180) / 360 * E_W).astype(int) % E_W
        lon_1km = np.floor((lons + 180) / 360 * W).astype(int) % W
        
        # Geographic Latitude MUST return blank space when querying past the Poles
        lat_idx_raw = np.floor((90 - lats) / 180 * E_H).astype(int)
        lat_1km_raw = np.floor((90 - lats) / 180 * H).astype(int)
        
        lat_idx = np.clip(lat_idx_raw, 0, E_H - 1)
        lat_1km = np.clip(lat_1km_raw, 0, H - 1)
        
        LON_IDX, LAT_IDX = np.meshgrid(lon_idx, lat_idx)
        LON_1KM, LAT_1KM = np.meshgrid(lon_1km, lat_1km)
        
        tile_precips = mean_precip[LAT_IDX, LON_IDX]
        clamped = np.clip(tile_precips, 0, 500).astype(int)
        rgba_img = precip_lut[clamped]
        
        # Determine valid map bounds (don't draw off into space vertically)
        LAT_VALID = (lat_idx_raw >= 0) & (lat_idx_raw < E_H)
        _, LAT_VALID_GRID = np.meshgrid(lon_idx, LAT_VALID)
        
        # Scrub out coastal artifacts and NaN regions that became exactly 0.0
        zero_mask = tile_precips == 0
        rgba_img[zero_mask] = [0, 0, 0, 0]

        # Transparent for oceans (No Data = 0 in master 1km file)
        water_mask = master_data[LAT_1KM, LON_1KM] == 0
        rgba_img[water_mask] = [0, 0, 0, 0]
        
        # Scrub vertical void
        rgba_img[np.logical_not(LAT_VALID_GRID)] = [0, 0, 0, 0]

        img = Image.fromarray(rgba_img, 'RGBA')
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        return buf.getvalue()

    def generate_tile(self, z, x, y):
        num_tiles = 2 ** z
        map_size = 256 * num_tiles

        # Generate vectors
        px = x * 256 + np.arange(256)
        x_norm = (px / map_size) - 0.5
        lons = 360 * x_norm
        
        py = y * 256 + np.arange(256)
        y_norm = 0.5 - (py / map_size)
        lats = 90 - 360 * np.arctan(np.exp(-y_norm * 2 * np.pi)) / np.pi
        
        # Exact mathematical mapping onto the 1km array
        # lon -180...180 maps to 0...W
        lon_idx = np.floor((lons + 180) / 360 * W).astype(int)
        lon_idx = np.clip(lon_idx, 0, W - 1)
        
        # lat 90...-90 maps to 0...H
        lat_idx = np.floor((90 - lats) / 180 * H).astype(int)
        lat_idx = np.clip(lat_idx, 0, H - 1)
        
        LON_IDX, LAT_IDX = np.meshgrid(lon_idx, lat_idx)
        
        tile_classes = master_data[LAT_IDX, LON_IDX]
        
        rgba_img = color_lut[tile_classes]
        img = Image.fromarray(rgba_img, 'RGBA')
        
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        return buf.getvalue()

if __name__ == '__main__':
    server = HTTPServer(('0.000.0', PORT), TileRequestHandler)
    print(f"Dynamic Tile Server running on http://localhost:{PORT}")
    server.serve_forever()
