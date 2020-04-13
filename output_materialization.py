import csv
import os

from dagster import (
    EventMetadataEntry,
    Field,
    Materialization,
    Selector,
    String,
    execute_pipeline,
    input_hydration_config,
    output_materialization_config,
    pipeline,
    seven,
    solid,
    usable_as_dagster_type,
)


@input_hydration_config(Selector({'csv': Field(String)}))
def less_simple_data_frame_input_hydration_config(context, selector):
    with open(selector['csv'], 'r') as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info('Read {n_lines} lines'.format(n_lines=len(lines)))
    return LessSimpleDataFrame(lines)


@output_materialization_config(
    {
        'csv': Field(
            {
                'path': String,
                'sep': Field(String, is_required=False, default_value=','),
            },
            is_required=False,
        ),
        'json': Field({'path': String,}, is_required=False,),
    }
)
def less_simple_data_frame_output_materialization_config(
    context, config, value
):
    # Materialize LessSimpleDataFrame into a csv file
    csv_path = os.path.abspath(config['csv']['path'])
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, 'w') as fd:
        fieldnames = list(value[0].keys())
        writer = csv.DictWriter(
            fd, fieldnames, delimiter=config['csv']['sep']
        )
        writer.writeheader()
        writer.writerows(value)

    context.log.debug(
        'Wrote dataframe as .csv to {path}'.format(path=csv_path)
    )
    yield Materialization(
        '1data_frame_csv',
        'LessSimpleDataFrame materialized as csv',
        [
            EventMetadataEntry.path(
                path=csv_path,
                label='data_frame_csv_path',
                description='LessSimpleDataFrame written to csv format',
            )
        ],
    )
    # Materialize LessSimpleDataFrame into a json file
    json_path = os.path.abspath(config['json']['path'])
    with open(json_path, 'w') as fd:
        json_value = seven.json.dumps([dict(row) for row in value])
        fd.write(json_value)

    context.log.debug(
        'Wrote dataframe as .json to {path}'.format(path=json_path)
    )
    yield Materialization(
        'data_frame_json',
        'LessSimpleDataFrame materialized as json',
        [
            EventMetadataEntry.path(
                path=json_path,
                label='data_frame_json_path',
                description='LessSimpleDataFrame written to json format',
            )
        ],
    )


@usable_as_dagster_type(
    name='LessSimpleDataFrame',
    description='A more sophisticated data frame that type checks its structure.',
    input_hydration_config=less_simple_data_frame_input_hydration_config,
    output_materialization_config=less_simple_data_frame_output_materialization_config,
)
class LessSimpleDataFrame(list):
    pass


@solid
def sort_by_calories(
    context, cereals: LessSimpleDataFrame
) -> LessSimpleDataFrame:
    sorted_cereals = sorted(
        cereals, key=lambda cereal: int(cereal['calories'])
    )
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
def output_materialization_pipeline():
    sort_by_calories()


if __name__ == '__main__':
    execute_pipeline(
        output_materialization_pipeline,
        {
            'solids': {
                'sort_by_calories': {
                    'inputs': {'cereals': {'csv': './data/cereal.csv'}},
                    'outputs': [
                        {
                            'result': {
                                'csv': {'path': 'output/cereal_out.csv'},
                                'json': {'path': 'output/cereal_out.json'},
                            }
                        }
                    ],
                }
            }
        },
    )