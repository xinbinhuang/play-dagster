import csv
import sqlite3
from copy import deepcopy

from dagster import (
    Field,
    ModeDefinition,
    String,
    execute_pipeline,
    pipeline,
    resource,
    solid,
    composite_solid
)


class LocalSQLiteWarehouse(object):
    def __init__(self, conn_str):
        self._conn_str = conn_str

    # In practice, you'll probably want to write more generic, reusable logic on your resources
    # than this tutorial example
    def update_normalized_cereals(self, records):
        conn = sqlite3.connect(self._conn_str)
        curs = conn.cursor()
        try:
            curs.execute('DROP TABLE IF EXISTS normalized_cereals')
            curs.execute(
                '''CREATE TABLE IF NOT EXISTS normalized_cereals
                (name text, mfr text, type text, calories real,
                 protein real, fat real, sodium real, fiber real,
                 carbo real, sugars real, potass real, vitamins real,
                 shelf real, weight real, cups real, rating real)'''
            )
            curs.executemany(
                '''INSERT INTO normalized_cereals VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                [tuple(record.values()) for record in records],
            )
        finally:
            curs.close()


@resource(config={'conn_str': Field(String)})
def local_sqlite_warehouse_resource(context):
    return LocalSQLiteWarehouse(context.resource_config['conn_str'])


@solid
def read_csv(context, csv_path):
    with open(csv_path, 'r') as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info('Read {n_lines} lines'.format(n_lines=len(lines)))
    return lines


@solid
def normalize_calories(context, cereals):
    columns_to_normalize = [
        'calories',
        'protein',
        'fat',
        'sodium',
        'fiber',
        'carbo',
        'sugars',
        'potass',
        'vitamins',
        'weight',
    ]
    quantities = [cereal['cups'] for cereal in cereals]
    reweights = [1.0 / float(quantity) for quantity in quantities]

    normalized_cereals = deepcopy(cereals)
    for idx in range(len(normalized_cereals)):
        cereal = normalized_cereals[idx]
        for column in columns_to_normalize:
            cereal[column] = float(cereal[column]) * reweights[idx]
    return normalized_cereals


@solid(required_resource_keys={'warehouse'})
def load_cereals(context, cereals):
    context.resources.warehouse.update_normalized_cereals(cereals)


@composite_solid
def load_normalized_cereals():
    cereals = read_csv()
    normalized_cereals = normalize_calories(cereals)
    load_cereals(normalized_cereals)


@pipeline(
    mode_defs=[
        ModeDefinition(
            resource_defs={'warehouse': local_sqlite_warehouse_resource}
        )
    ]
)
def resources_pipeline():
    load_normalized_cereals()


if __name__ == '__main__':
    environment_dict = {
        'solids': {
            'read_csv': {'inputs': {'csv_path': {'value': './data/cereal.csv'}}}
        },
        'resources': {'warehouse': {'config': {'conn_str': ':memory:'}}},
    }
    result = execute_pipeline(
        resources_pipeline, environment_dict=environment_dict
    )
    assert result.success
