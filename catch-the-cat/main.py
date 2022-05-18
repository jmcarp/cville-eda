#!/usr/bin/env python

import abc
import argparse
from datetime import datetime

import pytz
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

BASE_URL = "https://catpublic.etaspot.net/service.php"
TOKEN = "TESTING"

TIMEZONE = pytz.timezone("US/Eastern")

PROJECT_ID = "cvilledata"
DATASET_ID = "cat"

VEHICLES_TABLE_ID = "vehicles"
STOPS_TABLE_ID = "stops"
ROUTES_TABLE_ID = "routes"

VEHICLES_SCHEMA = [
    bigquery.SchemaField("nextStopExtID", "STRING", "NULLABLE"),
    bigquery.SchemaField("aID", "STRING", "NULLABLE"),
    bigquery.SchemaField("receiveTime", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("receiveTimestamp", "DATETIME", "NULLABLE"),
    bigquery.SchemaField("trainID", "STRING", "NULLABLE"),
    bigquery.SchemaField("vehicleType", "STRING", "NULLABLE"),
    bigquery.SchemaField("nextStopPctProg", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("onSchedule", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("inService", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("directionAbbr", "STRING", "NULLABLE"),
    bigquery.SchemaField("h", "INTEGER", "NULLABLE"),
    bigquery.SchemaField(
        "minutesToNextStops",
        "RECORD",
        "REPEATED",
        fields=(
            bigquery.SchemaField("routeID", "STRING", "NULLABLE"),
            bigquery.SchemaField("equipmentID", "STRING", "NULLABLE"),
            bigquery.SchemaField("directionAbbr", "STRING", "NULLABLE"),
            bigquery.SchemaField("time", "STRING", "NULLABLE"),
            bigquery.SchemaField("track", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("statuscolor", "STRING", "NULLABLE"),
            bigquery.SchemaField("scheduleNumber", "STRING", "NULLABLE"),
            bigquery.SchemaField("direction", "STRING", "NULLABLE"),
            bigquery.SchemaField("minutes", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("timePoint", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("patternStopID", "STRING", "NULLABLE"),
            bigquery.SchemaField("stopID", "STRING", "NULLABLE"),
            bigquery.SchemaField("status", "STRING", "NULLABLE"),
            bigquery.SchemaField("schedule", "STRING", "NULLABLE"),
            bigquery.SchemaField("blockID", "STRING", "NULLABLE"),
        ),
    ),
    bigquery.SchemaField("scheduleNumber", "STRING", "NULLABLE"),
    bigquery.SchemaField("nextStopID", "STRING", "NULLABLE"),
    bigquery.SchemaField("patternID", "STRING", "NULLABLE"),
    bigquery.SchemaField("seq", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("nextPatternStopID", "STRING", "NULLABLE"),
    bigquery.SchemaField("blockID", "STRING", "NULLABLE"),
    bigquery.SchemaField("direction", "STRING", "NULLABLE"),
    bigquery.SchemaField("capacity", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("tripID", "STRING", "NULLABLE"),
    bigquery.SchemaField("deadHead", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("lat", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("lng", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("load", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("routeID", "STRING", "NULLABLE"),
    bigquery.SchemaField("lastStopExtID", "STRING", "NULLABLE"),
    bigquery.SchemaField("equipmentID", "STRING", "NULLABLE"),
    bigquery.SchemaField("nextStopETA", "INTEGER", "NULLABLE"),
]

STOPS_SCHEMA = [
    bigquery.SchemaField("shortName", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("extID", "STRING", "NULLABLE"),
    bigquery.SchemaField("lat", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("name", "STRING", "NULLABLE"),
    bigquery.SchemaField("lng", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("id", "STRING", "NULLABLE"),
    bigquery.SchemaField("rid", "STRING", "NULLABLE"),
    bigquery.SchemaField("fetchTimestamp", "DATETIME", "NULLABLE"),
]

ROUTES_SCHEMA = [
    bigquery.SchemaField("showVehicleCapacity", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("showPlatform", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("showDirection", "BOOLEAN", "NULLABLE"),
    bigquery.SchemaField("vType", "STRING", "NULLABLE"),
    bigquery.SchemaField("order", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("type", "STRING", "NULLABLE"),
    bigquery.SchemaField("showScheduleNumber", "INTEGER", "NULLABLE"),
    bigquery.SchemaField("color", "STRING", "NULLABLE"),
    bigquery.SchemaField("encLine", "STRING", "NULLABLE"),
    bigquery.SchemaField("stops", "INTEGER", "REPEATED"),
    bigquery.SchemaField("abbr", "STRING", "NULLABLE"),
    bigquery.SchemaField("name", "STRING", "NULLABLE"),
    bigquery.SchemaField("id", "STRING", "NULLABLE"),
    bigquery.SchemaField("fetchTimestamp", "DATETIME", "NULLABLE"),
]


class Endpoint(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def fetch(self):
        pass

    @abc.abstractproperty
    def table_id(self):
        pass

    @abc.abstractproperty
    def table(self):
        pass


class Vehicles(Endpoint):
    def fetch(self):
        response = requests.get(
            BASE_URL,
            params={
                "service": "get_vehicles",
                "includeETAData": "1",
                "orderedETAArray": "1",
                "token": TOKEN,
            },
        )
        response.raise_for_status()
        vehicles = response.json()["get_vehicles"]
        for vehicle in vehicles:
            timestamp = datetime.fromtimestamp(vehicle["receiveTime"] / 1000, TIMEZONE)
            timestamp_naive = timestamp.replace(tzinfo=None)
            vehicle["receiveTimestamp"] = timestamp_naive.isoformat()
        return vehicles

    @property
    def table_id(self):
        return ".".join([PROJECT_ID, DATASET_ID, VEHICLES_TABLE_ID])

    @property
    def table(self):
        table = bigquery.Table(self.table_id, schema=VEHICLES_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="receiveTimestamp",
        )
        return table


class Routes(Endpoint):
    def fetch(self):
        # The routes endpoint doesn't include a timestamp, so we add our own
        fetch_timestamp = datetime.now(TIMEZONE).replace(tzinfo=None).isoformat()
        response = requests.get(
            BASE_URL,
            params={
                "service": "get_routes",
                "token": TOKEN,
            },
        )
        response.raise_for_status()
        routes = response.json()["get_routes"]
        for route in routes:
            route["fetchTimestamp"] = fetch_timestamp
        return routes

    @property
    def table_id(self):
        return ".".join([PROJECT_ID, DATASET_ID, ROUTES_TABLE_ID])

    @property
    def table(self):
        return bigquery.Table(self.table_id, schema=ROUTES_SCHEMA)


class Stops(Endpoint):
    def fetch(self):
        # The stops endpoint doesn't include a timestamp, so we add our own
        fetch_timestamp = datetime.now(TIMEZONE).replace(tzinfo=None).isoformat()
        response = requests.get(
            BASE_URL,
            params={
                "service": "get_stops",
                "token": TOKEN,
            },
        )
        response.raise_for_status()
        stops = response.json()["get_stops"]
        for stop in stops:
            stop["fetchTimestamp"] = fetch_timestamp
        return stops

    @property
    def table_id(self):
        return ".".join([PROJECT_ID, DATASET_ID, STOPS_TABLE_ID])

    @property
    def table(self):
        return bigquery.Table(self.table_id, schema=STOPS_SCHEMA)


ENDPOINT_TO_CLASS = {
    "vehicles": Vehicles,
    "routes": Routes,
    "stops": Stops,
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("endpoint", choices=ENDPOINT_TO_CLASS.keys())
    args = parser.parse_args()

    bq_client = bigquery.Client()
    endpoint = ENDPOINT_TO_CLASS[args.endpoint]()

    try:
        table = bq_client.get_table(endpoint.table_id)
    except NotFound:
        bq_client.create_table(endpoint.table)

    rows = endpoint.fetch()
    bq_client.insert_rows_json(endpoint.table, rows)
