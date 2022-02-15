#!/usr/bin/env bash

geojsonify() {
  local input=$1
  local output=$2
  local layer
  layer=$(ogrinfo "${input}" | grep '1: ' | awk '{print $2}')
  local query="${3:-"select *, asgeojson(geometry) as geometry from '${layer}'"}"
  ogr2ogr \
    -f csv \
    -dialect sqlite \
    -sql "${query}" \
    "${output}" \
    "${input}"
}
