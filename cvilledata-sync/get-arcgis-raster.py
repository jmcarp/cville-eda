#!/usr/bin/env python

import argparse
import io
import json

import geopandas as gpd
import pandas as pd
import rasterio
import rasterio.features
import requests
import shapely.geometry as sg

BASE_URL = "https://gisweb.charlottesville.org/arcgis/rest/services"


def layer_to_frame(layer_name, tile_bins=4, tile_resolution=3200):
    layer = get_layer(layer_name)
    shapes = []
    for tile in get_tiles(layer["extent"], tile_bins):
        image = get_image("CriticalSlopeLotRegulation", tile, [tile_resolution, tile_resolution])
        response = requests.get(image["href"])
        raster, tile_shapes = raster_to_shapes(io.BytesIO(response.content))
        shapes.extend(tile_shapes)
    df = gpd.GeoDataFrame.from_features(shapes)
    df = df.set_crs(raster.crs)
    return df


def get_layer(layer):
    response = requests.get(f"{BASE_URL}/{layer}/ImageServer", params={"f": "json"})
    response.raise_for_status()
    return response.json()


def get_tiles(extent, bins):
    x_range = extent["xmax"] - extent["xmin"]
    y_range = extent["ymax"] - extent["ymin"]
    x_bin_size = x_range / bins
    y_bin_size = y_range / bins
    for x in range(bins):
        x_coords = (
            extent["xmin"] + x_bin_size * x,
            extent["xmin"] + x_bin_size * (x + 1),
        )
        for y in range(bins):
            y_coords = (
                extent["ymin"] + y_bin_size * y,
                extent["ymin"] + y_bin_size * (y + 1),
            )
            yield [x_coords[0], y_coords[0], x_coords[1], y_coords[1]]


def get_image(layer, bbox, size):
    response = requests.get(
        f"{BASE_URL}/{layer}/ImageServer/exportImage",
        params={
            "bbox": ",".join(str(coord) for coord in bbox),
            "size": ",".join(str(dimension) for dimension in size),
            "format": "tiff",
            "f": "pjson",
        },
    )
    response.raise_for_status()
    return response.json()


def raster_to_shapes(raster):
    with rasterio.open(raster) as raster:
        image = raster.read(1)
    return raster, [
        {
            "properties": {"value": value},
            "geometry": shape,
        }
        for shape, value in rasterio.features.shapes(image, transform=raster.transform)
        if value
    ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", required=True)
    parser.add_argument("--outfile", required=True)
    parser.add_argument("--tile-bins", type=int, default=4)
    parser.add_argument("--tile-resolution", type=int, default=3200)
    args = parser.parse_args()

    df = layer_to_frame(args.layer, tile_bins=args.tile_bins, tile_resolution=args.tile_resolution)
    df = df.to_crs("WGS84")
    out_df = pd.DataFrame({
        "value": df.value,
        "geometry": df.geometry.apply(lambda shape: json.dumps(sg.mapping(shape))),
    })
    out_df.to_csv(args.outfile, index=False)
