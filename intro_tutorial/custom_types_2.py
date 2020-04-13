import csv

from dagster import execute_pipeline, pipeline, solid

from custom_types import LessSimpleDataFrame


@solid
def bad_read_csv(context, csv_path: str) -> LessSimpleDataFrame:
    with open(csv_path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))


@pipeline
def bad_custom_type_pipeline():
    bad_read_csv()


if __name__ == "__main__":

    execute_pipeline(
        bad_custom_type_pipeline,
        environment_dict={
            "solids": {
                "bad_read_csv": {"inputs": {"csv_path": {"value": "./data/cereal.csv"}}}
            }
        },
    )
