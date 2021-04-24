#!/bin/bash

# https://opendata.charlottesville.org/datasets/master-address-table
curl -o master-address-table.csv https://opendata.arcgis.com/datasets/dd9c7d93ed67438baefa3276c716f59d_5.csv

python geocode-addresses.py

bq load --autodetect --replace whatthecarp:cville_eda_raw.master_addresses_geocoded master-address-geocoded.csv
