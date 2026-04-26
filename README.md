## Layers

### Normal Layers
 - Köppen zone - access by clicking the Köppen description in the popup
 - Driest Month - click the underlined red month in the popup chart 
 - Wettest Month - click the underlined blue month in the popup chart 
 - Annual Average Temperature - click the top number in the year column in the chart
 - Annual Average Precipitation - click the bottom number in the year column in the chart
 - Monthly Temperature (1991-2020 set only) - click a number in the top row of the chart
 - Monthly Precipitation (1991-2020 set only) - click a number in the bottom row of the chart
 
### Compare layers
 > To access any compare layer, select a different set in the right dropdown from the left dropdown
 - Köppen change: click the popup header
 - Temperature change: click the underlined text on the left
 - Precipitation change: click the underlined text on the right

___

## Settings

### Theme
 > Pick between a dark or light theme

### Basemap
 > Choose a map that is placed beneath the climate data overlay. There are 7 options: Dark, Topo, Physical, Light, OSM, Satellite, and a plain black option.

### Projection
 > Choose whether the map is displayed using Mercator or Globe projection.
 
### Units
 > Choose the units for temperature, precipitation and elevation.

### Layer Opacity
 > Adjust the opacity of the climate data overlay.

### 3D Relief and Hillshade
 > Toggle on to enable 3D relief and hillshade.
 - 3D relief is not very performant, if you are having trouble with the map lagging, try toggling it off

### 3D Exaggeration
 > Multiply the height of the 3D terrain.
 - Set to 0 to disable 3D terrain but keep hillshade

### Mobile Pin Style (Mobile Only)
 > Choose the style of the pin when the map is clicked on mobile
 - Classic - conforms to the theme selection
 - High Contrast - opposite color of the color behind it

___

## Other Elements / Features

### Map link
 > Click the coordinates on the right of the popup to open those coordinates in Google Maps

### Reset button
 > Left of the setting icon, click this to reset the map view and layer settings

### Trewartha info
 > Shown beneath the koppen header in the popup.
 - The 4 digit code shows the trewartha zone and the universal thermal scale summer/winter class
 - The text next to the code is the description of the universal thermal scale letters

### Middle Click Navigation
 - Hold/Click the mouse wheel and drag up and down to zoom in and out
 - Hold/Click the mouse wheel and drag left and right to rotate the view

___
 
## Citations & Data Sources

### Climate Classifications
- **Köppen-Geiger maps**: Beck, H. E., T. R. McVicar, N. Vergopolan, A. Berg, N. J. Lutsko, A. Dufour, Z. Zeng, X. Jiang, A. I. J. M. van Dijk, and D. G. Miralles. [High-resolution (1 km) Köppen-Geiger maps for 1901–2099 based on constrained CMIP6 projections](https://www.nature.com/articles/s41597-023-02549-6). *Scientific Data* 10, 724 (2023).
- **Trewartha Classification**: Derived from the system developed by Glenn Thomas Trewartha.

### Elevation & Terrain Data
- **Global Elevation**: [Mapzen Terrarium](https://github.com/tilezen/joerd/blob/master/docs/formats.md#terrarium) data hosted on Amazon S3. (Attribution: Mapzen, OpenStreetMap contributors).

### Software & Libraries
- **Mapping Engine**: [MapLibre GL JS](https://maplibre.org/)
- **Built with**: [Antigravity](https://antigravity.google.com/)

### Basemap Credits
- **CartoDB**: [Dark Matter](https://carto.com/basemaps/) & [Light Matter](https://carto.com/basemaps/) (Attribution: &copy; [CARTO](https://carto.com/attributions), &copy; [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors)
- **Esri**: [World Physical Map](https://www.arcgis.com/home/item.html?id=c14112e4d07b4690a618471b058c49e1) & [Satellite Imagery](https://www.arcgis.com/home/item.html?id=10df2279f9684e4a9f6a7f08febac2a9) (Attribution: &copy; Esri, Earthstar Geographics, Garmin, FAO, METI/NASA, USGS)
- **OpenTopoMap**: [Topography](https://opentopomap.org/) (Attribution: &copy; [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors, SRTM)
- **OpenStreetMap**: [Standard OSM](https://www.openstreetmap.org/) (Attribution: &copy; [OpenStreetMap](https://www.openstreetmap.org/copyright) contributors)

*Inspired by [koppen.earth](https://koppen.earth)*