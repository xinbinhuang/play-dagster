import csv

from dagster import (
    Path,
    Selector,
    SerializationStrategy,
    execute_pipeline,
    input_hydration_config,
    pipeline,
    solid,
    usable_as_dagster_type,
)


class CsvSerializationStrategy(SerializationStrategy):
    def __init__(self):
        super().__init__(
            'csv_strategy', read_mode='r', write_mode='w'
        )

    def serialize(self, value, write_file_obj):
        fieldnames = value[0]
        writer = csv.DictWriter(write_file_obj, fieldnames)
        writer.writeheader()
        writer.writerows(value)

    def deserialize(self, read_file_obj):
        reader = csv.DictReader(read_file_obj)
        return LessSimpleDataFrame([row for row in reader])


@input_hydration_config(Selector({'pickle': Path}))
def less_simple_data_frame_input_hydration_config(context, selector):
    with open(selector['pickle'], 'r') as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info('Read {n_lines} lines'.format(n_lines=len(lines)))
    return LessSimpleDataFrame(lines)


@usable_as_dagster_type(
    name='LessSimpleDataFrame',
    description=(
        'A naive representation of a data frame, e.g., as returned by '
        'csv.DictReader.'
    ),
    serialization_strategy=CsvSerializationStrategy(),
    input_hydration_config=less_simple_data_frame_input_hydration_config,
)
class LessSimpleDataFrame(list):
    pass


@solid
def read_csv(context, csv_path: str) -> LessSimpleDataFrame:
    with open(csv_path, 'r') as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info('Read {n_lines} lines'.format(n_lines=len(lines)))
    return LessSimpleDataFrame(lines)


@solid
def sort_by_calories(context, cereals: LessSimpleDataFrame):
    sorted_cereals = sorted(cereals, key=lambda cereal: cereal['calories'])
    context.log.info(
        'Least caloric cereal: {least_caloric}'.format(
            least_caloric=sorted_cereals[0]['name']
        )
    )
    context.log.info(
        'Most caloric cereal: {most_caloric}'.format(
            most_caloric=sorted_cereals[-1]['name']
        )
    )
    return LessSimpleDataFrame(sorted_cereals)


@pipeline
def serialization_strategy_pipeline():
    sort_by_calories(read_csv())


if __name__ == '__main__':
    environment_dict = {
        'solids': {
            'read_csv': {'inputs': {'csv_path': {'value': './data/cereal.csv'}}}
        },
        'storage': {'filesystem': {}},
    }
    result = execute_pipeline(
        serialization_strategy_pipeline, environment_dict=environment_dict
    )
    assert result.success