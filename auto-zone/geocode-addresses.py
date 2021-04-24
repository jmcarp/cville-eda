#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import csv
import os
import re
import shelve

from geocodio import GeocodioClient


API_KEY = os.environ["GEOCODIO_API_KEY"]


def main(batch_size: int = 50, run_size: int = 1000):
    client = GeocodioClient(API_KEY)
    with open("master-address-table.csv") as fp:
        rows = list(csv.DictReader(fp))
    addresses = collections.defaultdict(list)
    for row in rows:
        addresses[format_address(row)].append(row["MasterAddressID"])
    with shelve.open("geocode.shelf") as shelf:
        todo = [address for address in addresses.keys() if address not in shelf]
        count = 0
        while count < min(run_size, len(todo)):
            stop = min(count + batch_size, run_size, len(todo))
            print(count, stop)
            batch = todo[count : stop]
            results = client.geocode(batch)
            shelf.update(zip(batch, results))
            shelf.sync()
            count += len(results)
        if len(shelf) == len(addresses):
            with open("master-address-geocoded.csv", "w") as fp:
                writer = csv.DictWriter(fp, ["address_id", "address", "latitude", "longitude"])
                writer.writeheader()
                for address, address_ids in addresses.items():
                    for address_id in address_ids:
                        writer.writerow({
                            "address_id": address_id,
                            "address": clean_whitespace(address),
                            "latitude": shelf[address]["results"][0]["location"]["lat"],
                            "longitude": shelf[address]["results"][0]["location"]["lng"],
                        })


def format_address(row: dict) -> str:
    return (
        f"{row['ST_NUMBER']} {row['PREDIR']} {row['ST_NAME']} {row['SUFFIX']} {row['POSTDIR']}, "
        f"Charlottesville, VA, {row['ZIP']}"
    )


def clean_whitespace(value: str) -> str:
    value = value.strip()
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"\s+,", ",", value)
    return value


if __name__ == "__main__":
    main()
