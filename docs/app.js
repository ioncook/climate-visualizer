// HARDWARE ACCELERATION & OPTIMIZATIONS
maplibregl.prewarm();
maplibregl.workerCount = Math.max(2, navigator.hardwareConcurrency || 2);

// Global State (Initialized early to prevent ReferenceErrors)
let queryAbortController = null;
let lastLatLng = null;
let isPopupOpen = false;
let currentMonth = "12";
let currentLayerType = "koppen";
let lastQueryData = null;
let lockedTooltipCoords = "";
let currentMarker = null;
let elevCache = {};
let currentBasemapId = "dark";

const COLORS = {
  1: "rgb(0, 0, 255)", 2: "rgb(0, 120, 255)", 3: "rgb(70, 170, 250)", 4: "rgb(255, 0, 0)",
  5: "rgb(255, 150, 150)", 6: "rgb(245, 165, 0)", 7: "rgb(255, 220, 100)", 8: "rgb(255, 255, 0)",
  9: "rgb(200, 200, 0)", 10: "rgb(150, 150, 0)", 11: "rgb(150, 255, 150)", 12: "rgb(100, 200, 100)",
  13: "rgb(50, 150, 50)", 14: "rgb(200, 255, 80)", 15: "rgb(100, 255, 80)", 16: "rgb(50, 200, 0)",
  17: "rgb(255, 0, 255)", 18: "rgb(200, 0, 200)", 19: "rgb(150, 50, 150)", 20: "rgb(150, 100, 150)",
  21: "rgb(170, 175, 255)", 22: "rgb(90, 120, 220)", 23: "rgb(75, 80, 180)", 24: "rgb(50, 0, 135)",
  25: "rgb(0, 255, 255)", 26: "rgb(55, 200, 255)", 27: "rgb(0, 125, 125)", 28: "rgb(0, 70, 95)",
  29: "rgb(178, 178, 178)", 30: "rgb(102, 102, 102)"
};

const LEGENDS = {
  1: ["Af", "Tropical rainforest"], 2: ["Am", "Tropical monsoon"], 3: ["Aw", "Tropical savanna"],
  4: ["BWh", "Arid desert, hot"], 5: ["BWk", "Arid desert, cold"], 6: ["BSh", "Semi-arid, hot"], 7: ["BSk", "Semi-arid, cold"],
  8: ["Csa", "Temperate, dry summer, hot summer"], 9: ["Csb", "Temperate, dry summer, warm summer"], 10: ["Csc", "Temperate, dry summer, cold summer"],
  11: ["Cwa", "Temperate, dry winter, hot summer"], 12: ["Cwb", "Temperate, dry winter, warm summer"], 13: ["Cwc", "Temperate, dry winter, cold summer"],
  14: ["Cfa", "Temperate, no dry season, hot summer"], 15: ["Cfb", "Temperate, no dry season, warm summer"], 16: ["Cfc", "Temperate, no dry season, cold summer"],
  17: ["Dsa", "Cold, dry summer, hot summer"], 18: ["Dsb", "Cold, dry summer, warm summer"], 19: ["Dsc", "Cold, dry summer, cold summer"], 20: ["Dsd", "Cold, dry summer, very cold winter"],
  21: ["Dwa", "Cold, dry winter, hot summer"], 22: ["Dwb", "Cold, dry winter, warm summer"], 23: ["Dwc", "Cold, dry winter, cold summer"], 24: ["Dwd", "Cold, dry winter, very cold winter"],
  25: ["Dfa", "Cold, no dry season, hot summer"], 26: ["Dfb", "Cold, no dry season, warm summer"], 27: ["Dfc", "Cold, no dry season, cold summer"], 28: ["Dfd", "Cold, no dry season, very cold winter"],
  29: ["ET", "Polar tundra / Highland"], 30: ["EF", "Polar frost / Ice cap"]
};

const urlParams = new URLSearchParams(window.location.search);
const initialLat = parseFloat(urlParams.get('lat')) || 20;
const initialLng = parseFloat(urlParams.get('lng')) || 0;
const initialZ = parseFloat(urlParams.get('z')) || 1.5;

const eraSelect = document.getElementById('era');
const compareSelect = document.getElementById('compare');
const basemapSelect = document.getElementById('basemap');
const unitsSelect = document.getElementById('units');
const opacitySlider = document.getElementById('layer-opacity');
const projectionSelect = document.getElementById('projection');

if (urlParams.get('era')) eraSelect.value = urlParams.get('era');
if (urlParams.get('comp')) compareSelect.value = urlParams.get('comp');

const map = new maplibregl.Map({
  container: 'map',
  style: {
    version: 8,
    sources: {},
    layers: []
  },
  center: [initialLng, initialLat],
  zoom: initialZ,
  maxZoom: 11,
  maxPitch: 75,
  pixelRatio: Math.min(window.devicePixelRatio, 2),
  projection: 'globe',
  antialias: false,
  trackResize: false,
  collectResourceTiming: false,
  attributionControl: true,

  transformRequest: (url, resourceType) => {
    if (resourceType === 'Tile' && (url.includes('tiles_koppen') || url.includes('compare'))) {
      const match = url.match(/\/(\d+)\/(\d+)\/(\d+)\.png/);
      if (match) {
        const z = parseInt(match[1]);
        const y = parseInt(match[3]);
        const total = Math.pow(2, z);
        if (z > 4 && (y < total * 0.05 || y > total * 0.95)) {
          return { url: 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=' };
        }
      }
    }
  }
});

const BASEMAPS = {
  dark: { url: 'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png', attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>' },
  topo: { url: 'https://a.tile.opentopomap.org/{z}/{x}/{y}.png', attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, SRTM' },
  physical: { url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Physical_Map/MapServer/tile/{z}/{y}/{x}', attribution: '&copy; Esri, Earthstar Geographics, Garmin, FAO, METI/NASA, USGS' },
  light: { url: 'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>' },
  osm: { url: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png', attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors' },
  satellite: { url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', attribution: '&copy; Esri, Earthstar Geographics' }
};

currentMonth = urlParams.get('m') || "12";
currentLayerType = urlParams.get('layer') || 'koppen';
lastLatLng = (urlParams.get('plat') && urlParams.get('plng')) ?
  { lat: parseFloat(urlParams.get('plat')), lng: parseFloat(urlParams.get('plng')) } : null;
isPopupOpen = urlParams.get('p') === '1' && lastLatLng !== null;

if (lastLatLng) {
  const latAbs = Math.abs(lastLatLng.lat).toFixed(2), lngAbs = Math.abs(lastLatLng.lng).toFixed(2);
  const latDir = lastLatLng.lat >= 0 ? 'N' : 'S', lngDir = lastLatLng.lng >= 0 ? 'E' : 'W';
  lockedTooltipCoords = `<div style="text-decoration: underline; cursor: pointer;" onclick="event.stopPropagation(); window.open('https://www.google.com/maps?q=${lastLatLng.lat},${lastLatLng.lng}', '_blank');"><div>${latAbs}° ${latDir}</div><div>${lngAbs}° ${lngDir}</div></div>`;
}

function setupBasemap(id) {
  // Toggle pure black background layer
  if (id === 'none') {
    if (!map.getLayer('black-basemap')) {
      map.addLayer({
        id: 'black-basemap',
        type: 'background',
        paint: { 'background-color': '#000000' }
      }, 'bottom-anchor');
    }
    map.setLayoutProperty('black-basemap', 'visibility', 'visible');
    if (map.getLayer('basemap-layer')) map.setLayoutProperty('basemap-layer', 'visibility', 'none');
    return;
  } else {
    if (map.getLayer('black-basemap')) map.setLayoutProperty('black-basemap', 'visibility', 'none');
  }

  const { url, attribution } = BASEMAPS[id];
  if (map.getLayer('basemap-layer')) map.removeLayer('basemap-layer');
  if (map.getSource('basemap-source')) map.removeSource('basemap-source');

  map.addSource('basemap-source', {
    type: 'raster',
    tiles: [url],
    tileSize: 256,
    attribution: attribution
  });

  map.addLayer({
    id: 'basemap-layer',
    type: 'raster',
    source: 'basemap-source',
    paint: { 'raster-fade-duration': 0 }
  }, 'middle-anchor');
}

basemapSelect.addEventListener('change', (e) => {
  currentBasemapId = basemapSelect.value;
  localStorage.setItem('climate_basemap', currentBasemapId);
  setupBasemap(currentBasemapId);
});

projectionSelect.addEventListener('change', () => {
  const proj = projectionSelect.value;
  localStorage.setItem('climate_projection', proj);
  if (proj === 'globe') {
    map.setProjection({ type: 'globe' });
    map.dragRotate.enable();
    map.dragPan.enable();
  } else {
    map.setProjection({ type: 'mercator' });
    map.setBearing(0);
    map.dragRotate.enable();
  }
  applyTerrainState();
  updateLayers();
});

map.on('load', () => {
  if (!map.getLayer('bottom-anchor')) map.addLayer({ id: 'bottom-anchor', type: 'background', paint: { 'background-opacity': 0 } });
  if (!map.getLayer('middle-anchor')) map.addLayer({ id: 'middle-anchor', type: 'background', paint: { 'background-opacity': 0 } });
  if (!map.getLayer('top-anchor')) map.addLayer({ id: 'top-anchor', type: 'background', paint: { 'background-opacity': 0 } });


  loadStoredSettings();

  const era = eraSelect.value;
  const comp = compareSelect.value;
  const isComp = comp !== 'none' && era !== comp && (currentMonth === "12" || currentLayerType === 'koppen');

  const erasValues = ["1901_1930", "1931_1960", "1961_1990", "1991_2020"];
  let era1 = era;
  let era2 = comp;
  if (isComp && erasValues.indexOf(era1) > erasValues.indexOf(era2)) [era1, era2] = [era2, era1];

  const tilePrefix = isComp ? `compare/${era1}_${era2}` : era;
  const baseUrl = window.location.origin + window.location.pathname.substring(0, window.location.pathname.lastIndexOf('/') + 1);
  const initialTileUrl = baseUrl + `${tilePrefix}/tiles_koppen/{z}/{x}/{y}.png?v=2`;

  if (!map.getSource('data-source')) {
    map.addSource('data-source', {
      type: 'raster',
      tiles: [initialTileUrl],
      tileSize: 256,
      maxzoom: 7
    });
  }

  map.addLayer({
    id: 'data-layer',
    type: 'raster',
    source: 'data-source',
    paint: {
      'raster-opacity': 0.75,
      'raster-resampling': 'linear',
      'raster-fade-duration': 0
    }
  });

  const proj = projectionSelect.value;
  if (proj === 'globe') {
    map.setProjection({ type: 'globe' });
    map.dragRotate.enable();
  } else {
    map.setProjection({ type: 'mercator' });
    map.setPitch(0);
    map.setBearing(0);
    map.dragRotate.enable();
  }

  if (!map.getSource('terrain-source')) {
    map.addSource('terrain-source', {
      'type': 'raster-dem',
      'tiles': ['https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png'],
      'tileSize': 512,
      'encoding': 'terrarium',
      'maxzoom': 14
    });
  }

  if (!map.getSource('hillshade-source')) {
    map.addSource('hillshade-source', {
      'type': 'raster-dem',
      'tiles': ['https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png'],
      'tileSize': 512,
      'encoding': 'terrarium',
      'maxzoom': 14
    });
  }

  if (!map.getLayer('hillshade-layer')) {
    map.addLayer({
      id: 'hillshade-layer',
      type: 'hillshade',
      source: 'hillshade-source',
      paint: {
        'hillshade-exaggeration': 0.4,
        'hillshade-shadow-color': 'rgba(0,0,0,0.5)',
        'hillshade-highlight-color': 'rgba(255,255,255,0.1)'
      }
    }); // Add at top
  }

  function applyTerrainState() {
    const show = document.getElementById('hillshade-check').checked;
    const exInput = document.getElementById('terrain-exaggeration');
    const viz = show ? 'visible' : 'none';

    if (map.getLayer('hillshade-layer')) map.setLayoutProperty('hillshade-layer', 'visibility', viz);

    const isMobile = window.innerWidth < 600;
    const isGlobe = projectionSelect.value === 'globe';

    if (show) {
      if (isMobile && isGlobe) {
        if (exInput.type !== 'text') {
          exInput.dataset.prev = exInput.value;
          exInput.type = 'text';
        }
        exInput.value = '0 (Globe)';
        exInput.disabled = true;
        map.setTerrain(null);
      } else {
        if (exInput.type === 'text') {
          exInput.type = 'number';
          exInput.value = exInput.dataset.prev || '2.0';
        }
        exInput.disabled = false;
        let exVal = parseFloat(exInput.value);
        if (isNaN(exVal)) exVal = 0;
        if (exVal > 0) map.setTerrain({ 'source': 'terrain-source', 'exaggeration': exVal });
        else map.setTerrain(null);
      }
    } else {
      if (exInput.type === 'text') {
        exInput.type = 'number';
        exInput.value = exInput.dataset.prev || '2.0';
      }
      exInput.disabled = false;
      map.setTerrain(null);
    }
    const globeOpt = Array.from(projectionSelect.options).find(o => o.value === 'globe');
    if (globeOpt) {
      globeOpt.disabled = false;
      globeOpt.text = 'Globe';
    }
  }

  window.applyTerrainState = applyTerrainState;
  const storedHill = localStorage.getItem('climate_hillshade') === 'true';
  document.getElementById('hillshade-check').checked = storedHill;
  applyTerrainState();
  updateLayers();

  const params = new URLSearchParams(window.location.search);
  const plat = parseFloat(params.get('plat'));
  const plng = parseFloat(params.get('plng'));
  if (!isNaN(plat) && !isNaN(plng) && params.get('p') === '1') {
    isPopupOpen = true;
    requestAnimationFrame(() => queryLocation(plat, plng));
  }
});


function isCompareMode() {
  const era = eraSelect.value;
  const comp = compareSelect.value;
  const supported = currentMonth == "12" || currentLayerType === 'koppen';
  return comp !== 'none' && comp !== era && supported;
}

document.getElementById('hillshade-check').addEventListener('change', (e) => {
  localStorage.setItem('climate_hillshade', e.target.checked);
  applyTerrainState();
});

const terrainExInput = document.getElementById('terrain-exaggeration');
terrainExInput.addEventListener('change', () => {
  const val = parseFloat(terrainExInput.value);
  localStorage.setItem('climate_exaggeration', val);
  if (document.getElementById('hillshade-check').checked) {
    map.setTerrain({ 'source': 'terrain-source', 'exaggeration': val });
  }
});
terrainExInput.addEventListener('wheel', (e) => {
  e.preventDefault();
  const delta = e.deltaY > 0 ? -0.5 : 0.5;
  let val = (parseFloat(terrainExInput.value) + delta);
  val = Math.max(0, Math.min(20, val));
  terrainExInput.value = val.toFixed(1);
  terrainExInput.dispatchEvent(new Event('change'));
});

function syncUrl() {
  if (!map) return;
  const center = map.getCenter();
  const zoom = map.getZoom().toFixed(2);
  const params = new URLSearchParams(window.location.search);
  params.set('lat', center.lat.toFixed(4));
  params.set('lng', center.lng.toFixed(4));
  params.set('z', zoom);
  params.set('layer', currentLayerType);
  params.set('m', currentMonth);
  params.set('era', eraSelect.value);
  params.set('comp', compareSelect.value);

  if (isPopupOpen && lastLatLng) {
    params.set('plat', lastLatLng.lat.toFixed(4));
    params.set('plng', lastLatLng.lng.toFixed(4));
    params.set('p', '1');
  } else {
    params.delete('plat');
    params.delete('plng');
    params.delete('p');
  }
  window.history.replaceState({}, '', `${window.location.pathname}?${params.toString()}`);
}

function updateLayers() {
  const era = eraSelect.value;
  const comp = compareSelect.value;
  const eras = ["1901_1930", "1931_1960", "1961_1990", "1991_2020"];
  const isCompare = isCompareMode();
  let era1 = era;
  let era2 = comp;
  if (isCompare && eras.indexOf(era1) > eras.indexOf(era2)) [era1, era2] = [era2, era1];
  const lType = currentLayerType;
  const m = currentMonth;
  const tilePrefix = isCompare ? `compare/${era1}_${era2}` : era;
  let tilePath = "";
  if (lType === 'koppen') tilePath = `${tilePrefix}/tiles_koppen/{z}/{x}/{y}.png?v=2`;
  else if (lType === 'precip') tilePath = `${tilePrefix}/tiles_precip/${m}/{z}/{x}/{y}.png?v=2`;
  else if (lType === 'temp') tilePath = `${tilePrefix}/tiles_temp/${m}/{z}/{x}/{y}.png?v=2`;
  else if (lType === 'precip_max') tilePath = `${era}/tiles_precip_max/{z}/{x}/{y}.png?v=2`;
  else if (lType === 'precip_min') tilePath = `${era}/tiles_precip_min/{z}/{x}/{y}.png?v=2`;

  const op = opacitySlider.value / 100;
  const mText = m == "12" ? "Annual" : ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][m];
  const statusEl = document.getElementById('status');

  if (lType === 'none') {
    statusEl.innerText = "Click map for data";
    updateLegend('none');
  } else if (lType === 'koppen') {
    statusEl.innerText = isCompare ? `Köppen Change [${era1.replace("_", "-")} vs ${era2.replace("_", "-")}]` : `Köppen DB Active [${era.replace("_", "-")}]`;
    updateLegend('koppen', isCompare);
  } else if (lType === 'precip') {
    statusEl.innerText = isCompare ? `Precipitation Change [${era1.replace("_", "-")} vs ${era2.replace("_", "-")}]` : `Precipitation Active (${mText}) [${era.replace("_", "-")}]`;
    updateLegend('precip', isCompare);
  } else if (lType === 'temp') {
    statusEl.innerText = isCompare ? `Temperature Change [${era1.replace("_", "-")} vs ${era2.replace("_", "-")}]` : `Temperature Active (${mText}) [${era.replace("_", "-")}]`;
    updateLegend('temp', isCompare);
  } else if (lType === 'precip_max') {
    statusEl.innerText = `Month of Greatest Precipitation [${era.replace("_", "-")}]`;
    updateLegend('precip_max');
  } else if (lType === 'precip_min') {
    statusEl.innerText = `Month of Least Precipitation [${era.replace("_", "-")}]`;
    updateLegend('precip_min');
  }

  const source = map.getSource('data-source');
  if (source && tilePath) {
    const baseUrl = window.location.origin + window.location.pathname.substring(0, window.location.pathname.lastIndexOf('/') + 1);
    requestAnimationFrame(() => {
      if (map.getSource('data-source')) {
        source.setTiles([baseUrl + tilePath]);
        map.setLayoutProperty('data-layer', 'visibility', 'visible');
        map.setPaintProperty('data-layer', 'raster-opacity', op);
      }
    });
  } else if (source) {
    map.setLayoutProperty('data-layer', 'visibility', 'none');
  }
  syncUrl();
}

function updateLegend(type, isCompare) {
  const titleEl = document.getElementById('layer-title');
  const contentEl = document.getElementById('legend-content');
  const isMetric = unitsSelect.value === 'metric';
  const mText = currentMonth === "12" ? "Annual" : ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'][currentMonth];

  if (type === 'none') { titleEl.innerText = ""; contentEl.innerHTML = ""; return; }

  if (type === 'koppen') {
    titleEl.innerText = isCompare ? "Köppen Category Shift" : "Köppen-Geiger Class";
    if (isCompare) {
      contentEl.innerHTML = `
        <div class="gradient-bar" style="background: linear-gradient(to right, #8000ff, #0000ff, #00ffff, #00ff00, #ffff00, #ff8000, #ff0000);"></div>
        <div class="legend-labels"><span>Cooler Shift</span><span>Same</span><span>Warmer Shift</span></div>
      `;
    } else contentEl.innerHTML = `<div style="font-size:10px; color:#888;">Color indicates climate zone classification</div>`;
  } else if (type === 'temp') {
    titleEl.innerText = isCompare ? "Temperature Change" : `${mText} Average Temperature`;
    const t1 = isMetric ? "-0.8°C" : "-1.5°F";
    const t2 = isMetric ? "+1.7°C" : "+3°F";
    const tMin = isMetric ? "-30°C" : "-22°F";
    const tMax = isMetric ? "30°C" : "86°F";
    const tempGradient = `linear-gradient(to right, 
      hsl(270, 100%, 65%), 
      hsl(225, 100%, 65%), 
      hsl(180, 100%, 65%), 
      hsl(135, 100%, 65%), 
      hsl(90, 100%, 65%), 
      hsl(45, 100%, 65%), 
      hsl(0, 100%, 65%))`;
    const tMid = isCompare ? "0" : (isMetric ? "0°" : "32°F");
    contentEl.innerHTML = `
      <div class="gradient-bar" style="background: ${tempGradient};"></div>
      <div class="legend-labels"><span>${isCompare ? t1 : tMin}</span><span>${tMid}</span><span>${isCompare ? t2 : tMax}</span></div>
    `;
  } else if (type === 'precip') {
    titleEl.innerText = isCompare ? "Precipitation Change" : `${mText} Average Precipitation`;
    if (isCompare) {
      contentEl.innerHTML = `
        <div class="gradient-bar" style="background: linear-gradient(to right, #ff0000, #00ff00, #0000ff);"></div>
        <div class="legend-labels"><span>Drier</span><span>Stable</span><span>Wetter</span></div>
      `;
    } else {
      const isAnnual = currentMonth === "12";
      const pMid = isMetric ? (isAnnual ? "2200mm" : "185mm") : (isAnnual ? "86in" : "7.3in");
      const pMax = isMetric ? (isAnnual ? "6000mm" : "500mm") : (isAnnual ? "240in" : "20in");
      contentEl.innerHTML = `
        <div class="gradient-bar" style="background: linear-gradient(to right, hsl(0, 100%, 65%) 0%, hsl(60, 100%, 65%) 11%, hsl(120, 100%, 65%) 31%, hsl(180, 100%, 65%) 56%, hsl(240, 100%, 65%) 83%, hsl(270, 100%, 65%) 100%);"></div>
        <div class="legend-labels"><span>0</span><span>${pMid}</span><span>${pMax}</span></div>
      `;
    }
  } else if (type === 'precip_max' || type === 'precip_min') {
    titleEl.innerText = type === 'precip_max' ? "Wettest Month" : "Driest Month";
    contentEl.innerHTML = `
      <div class="gradient-bar" style="background: linear-gradient(to right, #ff00ff, #0000ff, #00ff00, #ffff00, #ff0000);"></div>
      <div class="legend-labels"><span>Jan</span><span>Jul</span><span>Dec</span></div>
    `;
  }
}

updateLayers();

if (window.innerWidth >= 600) {
  let syncUrlTimeout;
  map.on('moveend', () => {
    clearTimeout(syncUrlTimeout);
    syncUrlTimeout = setTimeout(syncUrl, 150);
  });
}

map.on('popupopen', () => { isPopupOpen = true; syncUrl(); });
map.on('popupclose', () => {
  isPopupOpen = false;
  if (window.currentMarker) window.currentMarker.remove();
  syncUrl();
});

function maybeRefresh() {
  if (window.currentPopup && window.currentPopup.isOpen() && lastLatLng) {
    queryLocation(lastLatLng.lat, lastLatLng.lng, true);
  }
}

unitsSelect.addEventListener('change', () => {
  localStorage.setItem('climate_units', unitsSelect.value);
  updateLayers();
  if (window.currentPopup && window.currentPopup.isOpen()) updatePopup();
  updateLegend(currentLayerType, isCompareMode());
});

opacitySlider.addEventListener('input', () => {
  const op = opacitySlider.value / 100;
  localStorage.setItem('climate_opacity', opacitySlider.value);
  if (map.getLayer('data-layer')) map.setPaintProperty('data-layer', 'raster-opacity', op);
});

document.getElementById('opacity-row').addEventListener('wheel', (e) => {
  e.preventDefault();
  const delta = e.deltaY > 0 ? -5 : 5;
  opacitySlider.value = Math.min(100, Math.max(0, parseInt(opacitySlider.value) + delta));
  opacitySlider.dispatchEvent(new Event('input'));
});

eraSelect.addEventListener('change', () => { updateLayers(); maybeRefresh(); });
compareSelect.addEventListener('change', () => { updateLayers(); maybeRefresh(); });

window.toggleSettings = (e) => {
  e.stopPropagation();
  document.getElementById('settings-menu').classList.toggle('show');
};

window.addEventListener('click', () => {
  document.getElementById('settings-menu').classList.remove('show');
});

document.getElementById('settings-menu').addEventListener('click', (e) => e.stopPropagation());

function loadStoredSettings() {
  const storedTheme = localStorage.getItem('climate_theme');
  if (storedTheme) {
    document.getElementById('theme').value = storedTheme;
    if (storedTheme === 'light') document.body.classList.add('light-mode');
    else document.body.classList.remove('light-mode');
  }
  const storedBasemap = localStorage.getItem('climate_basemap');
  const storedUnits = localStorage.getItem('climate_units');
  const storedOpacity = localStorage.getItem('climate_opacity');
  const storedProjection = localStorage.getItem('climate_projection');

  if (storedBasemap) {
    basemapSelect.value = storedBasemap;
    currentBasemapId = storedBasemap;
  }
  setupBasemap(currentBasemapId);

  if (storedUnits) unitsSelect.value = storedUnits;
  if (storedOpacity) opacitySlider.value = storedOpacity;
  if (storedProjection) {
    projectionSelect.value = storedProjection;
    if (storedProjection === 'globe') map.setProjection({ type: 'globe' });
    else { map.setProjection({ type: 'mercator' }); map.dragRotate.disable(); }
  }

  document.getElementById('hillshade-check').checked = localStorage.getItem('climate_hillshade') === 'true';
  const storedExag = localStorage.getItem('climate_exaggeration');
  if (storedExag) document.getElementById('terrain-exaggeration').value = storedExag;
  const storedPinStyle = localStorage.getItem('climate_mobile_pin') || 'classic';
  const pinSelect = document.getElementById('mobile-pin-style');
  if (pinSelect) pinSelect.value = storedPinStyle;
}

const themeSelect = document.getElementById('theme');
themeSelect.addEventListener('change', () => {
  const val = themeSelect.value;
  if (val === 'light') document.body.classList.add('light-mode');
  else document.body.classList.remove('light-mode');
  localStorage.setItem('climate_theme', val);
  if (isPopupOpen) updatePopup();
});

const pinSelect = document.getElementById('mobile-pin-style');
if (pinSelect) {
  pinSelect.addEventListener('change', () => {
    localStorage.setItem('climate_mobile_pin', pinSelect.value);
    if (isPopupOpen) updatePopup();
  });
}

window.setLayer = (type, month) => {
  if (type) currentLayerType = type;
  if (month !== undefined) currentMonth = String(month);
  updateLayers();
  maybeRefresh();
};

function makeSelectScrollable(selectEl) {
  selectEl.addEventListener('wheel', (e) => {
    e.preventDefault();
    const delta = e.deltaY > 0 ? 1 : -1;
    let newIndex = selectEl.selectedIndex + delta;
    if (newIndex >= 0 && newIndex < selectEl.options.length) {
      selectEl.selectedIndex = newIndex;
      selectEl.dispatchEvent(new Event('change'));
    }
  }, { passive: false });
}

makeSelectScrollable(eraSelect);
makeSelectScrollable(compareSelect);
makeSelectScrollable(basemapSelect);
makeSelectScrollable(unitsSelect);
makeSelectScrollable(projectionSelect);

document.getElementById('status').innerText = "Click map to load high-res climate data";

function getTempColor(t) {
  let clamped = Math.max(-30, Math.min(30, t));
  let hue = 240 * (1 - (clamped + 30) / 60);
  return `hsl(${hue}, 100%, 65%)`;
}

function getPrecipColor(p) {
  let clamped = Math.max(0, Math.min(500, p));
  let curve = Math.pow(clamped / 500, 0.7);
  let hue = 270 * curve;
  return `hsl(${hue}, 100%, 65%)`;
}

function getTrewartha(t, p, lat) {
  if (!t || !p) return { code: "N/A" };
  const annT = t.reduce((a, b) => a + b, 0) / 12;
  const annP = p.reduce((a, b) => a + b, 0);
  const isNorth = lat >= 0;
  const summerIdx = isNorth ? [3, 4, 5, 6, 7, 8] : [0, 1, 2, 9, 10, 11];
  const winterIdx = isNorth ? [0, 1, 2, 9, 10, 11] : [3, 4, 5, 6, 7, 8];
  const pSum = summerIdx.reduce((a, i) => a + p[i], 0);
  const pWin = winterIdx.reduce((a, i) => a + p[i], 0);
  const pw = (pWin / Math.max(1, annP)) * 100;
  const maxT = Math.max(...t);
  const minT = Math.min(...t);
  const maxIdx = t.indexOf(maxT);
  const minIdx = t.indexOf(minT);
  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const warmMonth = monthNames[maxIdx];
  const coldMonth = monthNames[minIdx];

  function getThermalInfo(temp) {
    if (temp >= 35) return { l: 'i', w: 'Severely Hot' };
    if (temp >= 28) return { l: 'h', w: 'Very Hot' };
    if (temp >= 22.2) return { l: 'a', w: 'Hot' };
    if (temp >= 18) return { l: 'b', w: 'Warm' };
    if (temp >= 10) return { l: 'l', w: 'Mild' };
    if (temp >= 0.1) return { l: 'k', w: 'Cool' };
    if (temp > -10) return { l: 'o', w: 'Cold' };
    if (temp >= -24.9) return { l: 'c', w: 'Very Cold' };
    if (temp >= -39.9) return { l: 'd', w: 'Severely Cold' };
    return { l: 'e', w: 'Excessively Cold' };
  }
  const warmInfo = getThermalInfo(maxT);
  const coldInfo = getThermalInfo(minT);
  const thermalSub = warmInfo.l + coldInfo.l;
  const coldDesc = coldInfo.w;
  const warmDesc = warmInfo.w;
  const R_cm = 2.3 * annT - 0.64 * pw + 41;
  const R_mm = R_cm * 10;

  if (annP < R_mm) {
    const type = annP < R_mm / 2 ? "Bw" : "Bs";
    return { code: type + thermalSub, coldDesc, warmDesc, coldMonth, warmMonth };
  }
  const m10 = t.filter(v => v >= 10).length;
  let group = "";
  if (t.every(v => v >= 18)) group = "A";
  else if (m10 >= 8) group = "C";
  else if (m10 >= 4) group = "D";
  else if (m10 >= 1) group = "E";
  else return { code: (maxT > 0 ? "Ft" : "Fi") + thermalSub, coldDesc, warmDesc, coldMonth, warmMonth };

  let sub = "";
  const pMin = Math.min(...p);
  const sMin = Math.min(...summerIdx.map(i => p[i]));
  const wMin = Math.min(...winterIdx.map(i => p[i]));
  const sMax = Math.max(...summerIdx.map(i => p[i]));
  const wMax = Math.max(...winterIdx.map(i => p[i]));

  if (group === "A") {
    if (pMin >= 60) sub = "r";
    else if (sMax >= 10 * wMin) sub = "w";
    else if (wMax >= 10 * sMin) sub = "s";
    else sub = "m";
  } else if (group === "C") {
    if (wMax >= 3 * sMin && sMin < 40) sub = "s";
    else if (sMax >= 3 * wMin && wMin < 40) sub = "w";
    else sub = "f";
  } else if (group === "D") sub = minT > 0 ? "o" : "c";
  else if (group === "E") sub = minT > -10 ? "o" : "c";

  return { code: group + sub + thermalSub, coldDesc, warmDesc, coldMonth, warmMonth };
}

function updatePopup() {
  if (!lastQueryData || !lastLatLng) return;
  const { d1, d2, isCompare, era, compareEra } = lastQueryData;
  const latlng = lastLatLng;
  const tre = getTrewartha(d1.t, d1.p, latlng.lat);
  const info1 = LEGENDS[d1.i] || ["Unk", "Unknown"];
  const info2 = isCompare ? (LEGENDS[d2.i] || ["Unk", "Unknown"]) : null;
  const unitType = document.getElementById('units').value;
  const isMetric = unitType === 'metric';
  const totalP1 = d1.p.reduce((acc, curr) => acc + curr, 0);
  const avgT1 = d1.t.reduce((acc, curr) => acc + curr, 0) / d1.t.length;
  const totalP2 = isCompare ? d2.p.reduce((acc, curr) => acc + curr, 0) : null;
  const avgT2 = isCompare ? d2.t.reduce((acc, curr) => acc + curr, 0) / d2.t.length : null;

  let html = `<div style="min-width: 280px; font-family: inherit; color: var(--text);">`;
  const coordHtml = lockedTooltipCoords;

  if (isCompare) {
    html += `
      <div style="font-weight:700; border-bottom:1px solid var(--border); padding-bottom:6px; margin-bottom:8px; display: flex; align-items: center; min-height: 32px;">
        <div style="display: flex; align-items: center; cursor: pointer; gap: 4px; margin-top: -3px;" onclick="event.stopPropagation(); setLayer('koppen')">
          <span style="background:${COLORS[d1.i]}; color:#000; padding:3px 8px; border-radius:3px; font-size: 14px; text-decoration: underline;">${info1[0]}</span>
          <span style="margin: 0 4px; color: var(--text-dim); font-size: 16px;">&rarr;</span>
          <span style="background:${COLORS[d2.i]}; color:#000; padding:3px 8px; border-radius:3px; font-size: 14px; text-decoration: underline;">${info2[0]}</span>
        </div>
        <div style="text-align: right; font-size: 10px; color: var(--text-dim); margin-left: auto; line-height: 1.2;">
          ${coordHtml}
          <div style="color:var(--text-dim);">Elev: ${lastQueryData.elevation || 'N/A'}${(lastQueryData.elevation && lastQueryData.elevation !== '---' && lastQueryData.elevation !== 'N/A') ? (isMetric ? 'm' : 'ft') : ''}</div>
        </div>
      </div>`;
  } else {
    html += `
      <div style="font-weight:700; border-bottom:1px solid var(--border); padding-bottom:4px; margin-bottom:5px; display: flex; align-items: center;">
        <div style="display: flex; flex-direction: column; align-items: flex-start; gap: 2px;">
           <div style="display: flex; align-items: center; gap: 8px;">
             <span style="background:${COLORS[d1.i]}; color:#000; padding:2px 6px; border-radius:3px; text-decoration: underline; cursor: pointer;" onclick="event.stopPropagation(); setLayer('koppen')">${info1[0]}</span>
             <span style="margin-top: 1px; cursor: pointer;" onclick="event.stopPropagation(); setLayer('koppen')">${info1[1]}</span>
           </div>
           <div style="font-size: 10px; color: var(--text-dim);">
             ${tre.code} - <span style="color:#8da0cf">${tre.coldDesc}</span> <span style="color:var(--text-dim);">(${tre.coldMonth})</span> to <span style="color:#f1948a">${tre.warmDesc}</span> <span style="color:var(--text-dim);">(${tre.warmMonth})</span>
           </div>
        </div>
        <div style="text-align: right; font-size: 10px; color: var(--text-dim); margin-left: auto; line-height: 1.2; align-self: center; margin-top: 2px;">
          ${coordHtml}
          <div style="color:var(--text-dim);">Elev: ${lastQueryData.elevation || 'N/A'}${(lastQueryData.elevation && lastQueryData.elevation !== '---' && lastQueryData.elevation !== 'N/A') ? (isMetric ? 'm' : 'ft') : ''}</div>
        </div>
      </div>`;
  }

  if (isCompare) {
    const dT = avgT2 - avgT1;
    const dP = ((totalP2 - totalP1) / Math.max(totalP1, 1)) * 100;
    // Match map colors (HSL logic from build_compare.py)
    const isModern = era === '1991_2020';
    const isDark = document.body.classList.contains('light-mode') === false;
    const lightness = isDark ? '65%' : '45%';
    const dTf = dT * 1.8; // dT is always in C in our data, so *1.8 for F delta
    const tHue = dTf >= 0 ? 120 * (1 - Math.min(dTf / 3.0, 1)) : 120 + 120 * Math.min(Math.abs(dTf) / 1.5, 1);
    const tColor = `hsl(${tHue}, 100%, ${lightness})`;

    const pMag = Math.min(Math.log10(Math.abs(dP) + 1) / 2.0, 1);
    const pHue = dP >= 0 ? 120 + 120 * pMag : 120 * (1 - pMag);
    const pColor = `hsl(${pHue}, 100%, ${lightness})`;

    html += `
      <div style="display: flex; justify-content: space-around; padding: 10px 0; text-align: center;">
        <div style="cursor: pointer;" onclick="setLayer('temp', 12)">
          <div style="font-size: 10px; color: var(--text-dim); margin-bottom: 4px;">TEMP CHANGE</div>
          <div style="font-size: 18px; font-weight: bold; color: ${tColor}; text-decoration: underline;">${dT > 0 ? '+' : ''}${isMetric ? dT.toFixed(1) : (dT * 1.8).toFixed(1)}°</div>
        </div>
        <div style="cursor: pointer;" onclick="setLayer('precip', 12)">
          <div style="font-size: 10px; color: var(--text-dim); margin-bottom: 4px;">PRECIP CHANGE</div>
          <div style="font-size: 18px; font-weight: bold; color: ${pColor}; text-decoration: underline;">${dP > 0 ? '+' : ''}${dP.toFixed(1)}%</div>
        </div>
      </div>
    `;
  } else {
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const isModern = era === '1991_2020';
    const maxP = Math.max(...d1.p), minP = Math.min(...d1.p);
    const maxIdx = d1.p.indexOf(maxP), minIdx = d1.p.indexOf(minP);
    html += `
      <table style="width:100%; font-size:11px; border-collapse:collapse; text-align:center;">
        <tr style="color:var(--text-dim); border-bottom:1px solid var(--border);">
          <td style="text-align:left; font-weight:bold; padding:2px 0;"></td>
          ${months.map((m, i) => {
      let style = "", click = isModern ? `setLayer('temp', ${i})` : "", cursor = isModern ? "cursor:pointer" : "cursor:default";
      if (i === maxIdx) { style = `color:#8da0cf; text-decoration:underline;`; click = `setLayer('precip_max')`; cursor = "cursor:pointer"; }
      else if (i === minIdx) { style = `color:#f1948a; text-decoration:underline;`; click = `setLayer('precip_min')`; cursor = "cursor:pointer"; }
      return `<td style="padding: 0 2px; ${cursor}; ${style}" onclick="${click}">${m}</td>`;
    }).join('')}
          <td style="border-left: 1px solid var(--border); font-weight: bold; padding-left: 4px;">Year</td>
        </tr>
        <tr style="cursor: pointer;" onclick="setLayer('temp', 12)">
          <td style="text-align:left; color:var(--text-dim); font-weight:bold; padding:2px 0;">T°</td>
          ${d1.t.map((t, i) => {
      const decoration = isModern ? "text-decoration: underline; cursor: pointer;" : "cursor: default;";
      const click = isModern ? `event.stopPropagation(); setLayer('temp', ${i})` : "event.stopPropagation();";
      return `<td style="color:${getTempColor(t)}; font-weight:bold; ${decoration}" onclick="${click}">${isMetric ? Math.round(t) : Math.round(t * 1.8 + 32)}</td>`;
    }).join('')}
          <td style="border-left: 1px solid var(--border); font-weight: bold; text-decoration: underline; cursor: pointer; color:${getTempColor(avgT1)};" onclick="event.stopPropagation(); setLayer('temp', 12)">${isMetric ? Math.round(avgT1) : Math.round(avgT1 * 1.8 + 32)}</td>
        </tr>
        <tr style="cursor: pointer;" onclick="setLayer('precip', 12)">
          <td style="text-align:left; color:var(--text-dim); font-weight:bold; padding:2px 0;">P</td>
          ${d1.p.map((p, i) => {
      const decoration = isModern ? "text-decoration: underline; cursor: pointer;" : "cursor: default;";
      const click = isModern ? `event.stopPropagation(); setLayer('precip', ${i})` : "event.stopPropagation();";
      return `<td style="color:${getPrecipColor(p)}; font-weight:bold; ${decoration}" onclick="${click}">${isMetric ? Math.round(p) : (p / 25.4).toFixed(1)}</td>`;
    }).join('')}
          <td style="border-left: 1px solid var(--border); font-weight: bold; text-decoration: underline; cursor: pointer; color:${getPrecipColor(totalP1 / 12)};" onclick="event.stopPropagation(); setLayer('precip', 12)">${isMetric ? Math.round(totalP1) : (totalP1 / 25.4).toFixed(1)}</td>
        </tr>
      </table>
    `;
  }
  html += `</div>`;
  if (window.currentPopup && window.currentPopup.isOpen()) window.currentPopup.setHTML(html);
  else {
    if (window.currentPopup) window.currentPopup.remove();
    window.currentPopup = new maplibregl.Popup({ maxWidth: '450px', className: 'climate-popup', anchor: 'bottom', autoPan: false })
      .setLngLat([latlng.lng, latlng.lat]).setHTML(html).addTo(map);
  }

  window.currentPopup.off('close');
  window.currentPopup.on('close', () => {
    document.querySelectorAll('.maplibregl-marker').forEach(m => m.remove());
    if (window.currentMarker) window.currentMarker = null;
  });

  if (window.currentMarker) window.currentMarker.remove();
  if (window.innerWidth < 600) {
    const el = document.createElement('div'); el.style.width = '24px'; el.style.height = '34px';
    const pinStyle = document.getElementById('mobile-pin-style') ? document.getElementById('mobile-pin-style').value : (localStorage.getItem('climate_mobile_pin') || 'classic');
    if (pinStyle === 'contrast') { el.style.mixBlendMode = 'difference'; el.innerHTML = `<svg width="24" height="34" viewBox="0 -1 24 33"><path d="M12 0C5.37 0 0 5.37 0 12c0 9 12 20 12 20s12-11 12-20c0-6.63-5.37-12-12-12zm0 18c-3.31 0-6-2.69-6-6s2.69-6 6-6 6 2.69 6 6-2.69 6-6 6z" fill="#ffffff" fill-rule="evenodd" /></svg>`; }
    else el.innerHTML = `<svg width="24" height="34" viewBox="0 -1 24 33" style="filter: drop-shadow(0 2px 2px rgba(0,0,0,0.4));"><path d="M12 0C5.37 0 0 5.37 0 12c0 9 12 20 12 20s12-11 12-20c0-6.63-5.37-12-12-12zm0 18c-3.31 0-6-2.69-6-6s2.69-6 6-6 6 2.69 6 6-2.69 6-6 6z" fill="var(--pin-color)" fill-rule="evenodd" /></svg>`;
    window.currentMarker = new maplibregl.Marker({ element: el, anchor: 'bottom' }).setLngLat([latlng.lng, latlng.lat]).addTo(map);
  }
  syncUrl();
}

const queryCache = {};

async function fetchJson(url, signal) {
  if (queryCache[url]) return queryCache[url];
  try {
    const res = await fetch(url, { signal });
    if (!res.ok) return null;
    queryCache[url] = await res.json();
    return queryCache[url];
  } catch (e) { return null; }
}

function decodeRle(rle, px, py) {
  if (!rle || !rle[py]) return 0;
  const row = rle[py];
  let cur = 0;
  for (let i = 0; i < row.length; i++) {
    let count = row[i][0];
    if (px < cur + count) return row[i][1];
    cur += count;
  }
  return 0;
}

async function queryLocation(lat, lng, isRefresh = false) {
  if (!lat) return;
  const wrappedLng = ((lng + 180) % 360 + 360) % 360 - 180;
  const clampedLat = Math.max(-89.9, Math.min(89.9, lat));
  if (queryAbortController) queryAbortController.abort();
  queryAbortController = new AbortController();
  const signal = queryAbortController.signal;
  lastQueryData = null;
  const eras = ["1901_1930", "1931_1960", "1961_1990", "1991_2020"];
  let era1 = eraSelect.value, era2 = compareSelect.value;
  if (era2 !== 'none' && eras.indexOf(era1) > eras.indexOf(era2)) [era1, era2] = [era2, era1];
  const isComp = era2 !== 'none' && era1 !== era2;

  const map_size = 256 * Math.pow(2, 7);
  let x_norm = (wrappedLng + 180) / 360;
  let y_norm = 0.5 - (Math.log(Math.tan(Math.PI / 4 + (clampedLat * Math.PI / 180) / 2)) / (2 * Math.PI));
  let px = Math.floor(x_norm * map_size), py = Math.floor(y_norm * map_size);
  if (py < 0 || py >= map_size) return;

  let tileX = Math.floor(px / 256), tileY = Math.floor(py / 256);
  let pixelX = px % 256, pixelY = py % 256;
  const latC = Math.floor(clampedLat / 10) * 10, lonC = Math.floor(wrappedLng / 10) * 10;
  const gridY = Math.floor((90 - clampedLat) / 180 * 1800), gridX = Math.floor((wrappedLng + 180) / 360 * 3600) % 3600;

  try {
    document.getElementById('status').innerText = `Loading...`;
    const [rle1, rle2, grid1, grid2] = await Promise.all([
      fetchJson(`${era1}/koppen_rle/7/${tileX}/${tileY}.json`, signal),
      isComp ? fetchJson(`${era2}/koppen_rle/7/${tileX}/${tileY}.json`, signal) : Promise.resolve(null),
      fetchJson(`${era1}/climate_grid/${latC}_${lonC}.json`, signal),
      isComp ? fetchJson(`${era2}/climate_grid/${latC}_${lonC}.json`, signal) : Promise.resolve(null)
    ]);
    if (signal.aborted) return;

    let kid1 = decodeRle(rle1, pixelX, pixelY);
    let kid2 = isComp ? decodeRle(rle2, pixelX, pixelY) : 0;
    let d1_raw = grid1 ? grid1[`${gridY}_${gridX}`] : null;
    let d2_raw = (isComp && grid2) ? grid2[`${gridY}_${gridX}`] : null;

    // COASTLINE SNAP: If initial hit is water, search in a 3-pixel radius (~3km) for the nearest land pixel.
    if (!kid1 || !d1_raw) {
      let found = false;
      const radius = 3;
      for (let r = 1; r <= radius && !found; r++) {
        for (let dy = -r; dy <= r && !found; dy++) {
          for (let dx = -r; dx <= r && !found; dx++) {
            if (Math.abs(dx) !== r && Math.abs(dy) !== r) continue;
            const nk1 = decodeRle(rle1, pixelX + dx, pixelY + dy);
            const nd1 = grid1 ? grid1[`${gridY + dy}_${(gridX + dx + 3600) % 3600}`] : null;
            if (nk1 && nd1) {
              kid1 = nk1; d1_raw = nd1;
              if (isComp) {
                kid2 = decodeRle(rle2, pixelX + dx, pixelY + dy);
                d2_raw = grid2 ? grid2[`${gridY + dy}_${(gridX + dx + 3600) % 3600}`] : null;
              }
              found = true;
            }
          }
        }
      }
    }

    if (!kid1 || !d1_raw) {
      document.getElementById('status').innerText = "Ocean (No Data)";
      if (window.currentPopup) window.currentPopup.remove();
      document.querySelectorAll('.maplibregl-marker').forEach(m => m.remove());
      if (window.currentMarker) { window.currentMarker.remove(); window.currentMarker = null; }
      return;
    }

    lastQueryData = {
      d1: { i: kid1, t: d1_raw.t, p: d1_raw.p },
      d2: (isComp && d2_raw) ? { i: kid2 || kid1, t: d2_raw.t, p: d2_raw.p } : null,
      isCompare: isComp && d2_raw, era: eraSelect.value, compareEra: compareSelect.value, elevation: "---"
    };
    document.getElementById('status').innerText = isComp ? "Comparison Data Ready" : "Data Ready";
    updatePopup();

    const elevKey = `${clampedLat.toFixed(3)}_${wrappedLng.toFixed(3)}`;
    if (window.elevCache && window.elevCache[elevKey]) { lastQueryData.elevation = window.elevCache[elevKey]; updatePopup(); }
    else {
      if (!window.elevCache) window.elevCache = {};
      const elevUrl = `https://api.open-meteo.com/v1/elevation?latitude=${clampedLat}&longitude=${wrappedLng}`;
      const timeoutId = setTimeout(() => { if (lastQueryData && lastQueryData.elevation === "---") { lastQueryData.elevation = null; updatePopup(); } }, 3000);
      fetch(elevUrl, { signal }).then(r => { clearTimeout(timeoutId); return r.ok ? r.json() : null; }).then(elevData => {
        if (signal.aborted || !lastQueryData) return;
        if (elevData && elevData.elevation && elevData.elevation[0] !== undefined) {
          let val = elevData.elevation[0];
          if (document.getElementById('units').value === 'imperial') val = val * 3.28084;
          const rounded = Math.round(val);
          lastQueryData.elevation = rounded; window.elevCache[elevKey] = rounded; updatePopup();
        } else { lastQueryData.elevation = null; updatePopup(); }
      }).catch(() => { if (lastQueryData) { lastQueryData.elevation = null; updatePopup(); } });
    }
  } catch (err) {
    if (err.name !== 'AbortError') {
      document.getElementById('status').innerText = "Ocean (No Data)";
      if (window.currentPopup) window.currentPopup.remove();
    }
  }
}

map.on('click', (e) => {
  if (map.isMoving() || map.isRotating() || map.isZooming()) return;
  const lng = e.lngLat.lng, lat = e.lngLat.lat;
  const wrappedLng = ((lng + 180) % 360 + 360) % 360 - 180;
  const clampedLat = Math.max(-90, Math.min(90, lat));
  const latAbs = Math.abs(clampedLat).toFixed(2), lngAbs = Math.abs(wrappedLng).toFixed(2);
  const latDir = clampedLat >= 0 ? 'N' : 'S', lngDir = wrappedLng >= 0 ? 'E' : 'W';
  lockedTooltipCoords = `<div style="text-decoration: underline; cursor: pointer;" onclick="event.stopPropagation(); window.open('https://www.google.com/maps?q=${clampedLat},${wrappedLng}', '_blank');"><div>${latAbs}° ${latDir}</div><div>${lngAbs}° ${lngDir}</div></div>`;
  lastLatLng = { lat: clampedLat, lng: wrappedLng }; isPopupOpen = true; syncUrl(); queryLocation(lat, lng);
});

let lastRightClickTime = 0;
map.on('contextmenu', (e) => {
  if (window.innerWidth < 600 || e.originalEvent?.pointerType === 'touch') return;
  const now = Date.now();
  if (now - lastRightClickTime < 500) { e.originalEvent.preventDefault(); map.easeTo({ pitch: 0, bearing: 0, duration: 800 }); }
  lastRightClickTime = now;
});

let isNavActive = false, middleStartPos = { x: 0, y: 0 }, middleCurrentPos = { x: 0, y: 0 }, middleStartLlngLat = null, isDragging = false, smoothedProj = null, navSmoothX = 0, navSmoothY = 0, middleStartTime = 0;
const crosshair = document.getElementById('nav-crosshair');

map.getCanvas().addEventListener('pointerdown', (e) => {
  if (window.innerWidth < 600) return;
  document.body.classList.add('grabbing');
  if (e.button === 1 && e.pointerType !== 'touch') {
    e.preventDefault();
    if (isNavActive) { middleStartTime = Date.now(); middleStartPos = { x: e.clientX, y: e.clientY }; isDragging = false; return; }
    middleStartTime = Date.now(); middleStartPos = { x: e.clientX, y: e.clientY }; middleCurrentPos = { x: e.clientX, y: e.clientY };
    navSmoothX = 0; navSmoothY = 0; isDragging = false; isNavActive = true;
    const rect = map.getCanvas().getBoundingClientRect();
    const pivot = map.unproject([e.clientX - rect.left, e.clientY - rect.top]);
    middleStartLlngLat = (!isNaN(pivot.lng) && !isNaN(pivot.lat)) ? pivot : null;
    smoothedProj = null; crosshair.style.left = e.clientX + 'px'; crosshair.style.top = e.clientY + 'px'; crosshair.style.display = 'block';
    map.triggerRepaint();
  }
});

window.addEventListener('pointermove', (e) => {
  if (isNavActive) {
    middleCurrentPos = { x: e.clientX, y: e.clientY };
    const dx = e.clientX - middleStartPos.x, dy = e.clientY - middleStartPos.y;
    const dist = Math.hypot(dx, dy);
    const directions = ['n', 'ne', 'e', 'se', 's', 'sw', 'w', 'nw'];
    directions.forEach(d => document.body.classList.remove('nav-' + d));
    if (dist > 10) {
      isDragging = true;
      const ax = Math.abs(dx), ay = Math.abs(dy);
      let dir = '';
      if (ay > ax * 1.8) dir = dy > 0 ? 's' : 'n';
      else if (ax > ay * 1.8) dir = dx > 0 ? 'e' : 'w';
      else { if (dx > 0) dir = dy > 0 ? 'se' : 'ne'; else dir = dy > 0 ? 'sw' : 'nw'; }
      document.body.classList.add('nav-' + dir);
    }
  }
});

window.addEventListener('pointerup', (e) => {
  document.body.classList.remove('grabbing');
  ['n', 'ne', 'e', 'se', 's', 'sw', 'w', 'nw'].forEach(d => document.body.classList.remove('nav-' + d));
  if (e.button === 1 && isNavActive) {
    if (isDragging || Date.now() - middleStartTime > 300) { isNavActive = false; crosshair.style.display = 'none'; }
  } else if (e.button === 0 && isNavActive) { isNavActive = false; crosshair.style.display = 'none'; }
  if (!isNavActive) { ['n', 'ne', 'e', 'se', 's', 'sw', 'w', 'nw'].forEach(d => document.body.classList.remove('nav-' + d)); currentNavDir = ''; }
});

let currentNavDir = '';
map.on('render', () => {
  if (!isNavActive) return;
  let dx = middleCurrentPos.x - middleStartPos.x, dy = middleCurrentPos.y - middleStartPos.y;
  if (Math.abs(dx) < 5 && Math.abs(dy) < 5) { dx = 0; dy = 0; }
  else { if (Math.abs(dy) > Math.abs(dx) * 10.0) dx = 0; else if (Math.abs(dx) > Math.abs(dy) * 10.0) dy = 0; }

  let nextDir = '';
  if (dx !== 0 || dy !== 0) {
    if (dx === 0) nextDir = dy > 0 ? 's' : 'n';
    else if (dy === 0) nextDir = dx > 0 ? 'e' : 'w';
    else { if (dx > 0) nextDir = dy > 0 ? 'se' : 'ne'; else nextDir = dy > 0 ? 'sw' : 'nw'; }
  }
  if (nextDir !== currentNavDir) {
    ['n', 'ne', 'e', 'se', 's', 'sw', 'w', 'nw'].forEach(d => document.body.classList.remove('nav-' + d));
    if (nextDir) document.body.classList.add('nav-' + nextDir);
    currentNavDir = nextDir;
  }

  navSmoothX = navSmoothX * 0.8 + dx * 0.2; navSmoothY = navSmoothY * 0.8 + dy * 0.2;
  if (navSmoothX !== 0 || navSmoothY !== 0) {
    map.jumpTo({ zoom: Math.max(1.5, map.getZoom() + (-navSmoothY * 0.0005)), bearing: map.getBearing() + (navSmoothX * 0.012) });
    if (middleStartLlngLat) {
      const rect = map.getCanvas().getBoundingClientRect();
      const rawProj = map.project(middleStartLlngLat);
      if (!smoothedProj) smoothedProj = { x: rawProj.x, y: rawProj.y };
      else {
        const rotStr = Math.min(0.3, Math.abs(navSmoothX) * 0.01);
        const k = 0.5 + rotStr;
        smoothedProj.x = smoothedProj.x * k + rawProj.x * (1 - k); smoothedProj.y = smoothedProj.y * k + rawProj.y * (1 - k);
      }
      const offX = (smoothedProj.x - (middleStartPos.x - rect.left)) * 0.8;
      const offY = (smoothedProj.y - (middleStartPos.y - rect.top)) * 0.8;
      if (!isNaN(offX) && (Math.abs(offX) > 0.05 || Math.abs(offY) > 0.05)) {
        if (Math.abs(offX) < 150 && Math.abs(offY) < 150) map.panBy([offX, offY], { duration: 0 });
      }
    }
  }
  map.triggerRepaint();
});
window.addEventListener('keydown', (e) => { if (e.key === 'Escape' && window.currentPopup) window.currentPopup.remove(); });
