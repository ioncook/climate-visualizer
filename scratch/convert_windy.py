import colorsys

def rgb_to_hsl(r, g, b):
    h, l, s = colorsys.rgb_to_hls(r/255.0, g/255.0, b/255.0)
    return round(h * 360, 1), round(s * 100, 1), round(l * 100, 1)

windy = [
    [203, [115, 70, 105, 255]],
    [218, [202, 172, 195, 255]],
    [233, [162, 70, 145, 255]],
    [248, [143, 89, 169, 255]],
    [258, [157, 219, 217, 255]],
    [265, [106, 191, 181, 255]],
    [269, [100, 166, 189, 255]],
    [273.15, [93, 133, 198, 255]],
    [274, [68, 125, 99, 255]],
    [283, [128, 147, 24, 255]],
    [294, [243, 183, 4, 255]],
    [303, [232, 83, 25, 255]],
    [320, [71, 14, 0, 255]]
]

results = []
for k, rgba in windy:
    c = round(k - 273.15, 2)
    h, s, l = rgb_to_hsl(rgba[0], rgba[1], rgba[2])
    results.append({"t": c, "h": h, "s": s, "l": l})

import json
print(json.dumps(results, indent=2))
