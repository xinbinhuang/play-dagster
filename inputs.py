import csv
from typing import Dict

from dagster import execute_pipeline, pipeline, solid


@solid
def read_csv(context, csv_path: str):
    with open(csv_path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))
    return lines


@solid
def sort_by_calories(context, cereals) -> Dict[str, dict]:
    sorted_cereals = sorted(cereals, key=lambda cereal: cereal["calories"])
    context.log.info(
        "Least caloric cereal: {least_caloric}".format(
            least_caloric=sorted_cereals[0]["name"]
        )
    )
    context.log.info(
        "Most caloric cereal: {most_caloric}".format(
            most_caloric=sorted_cereals[-1]["name"]
        )
    )
    return {
        "least_caloric": sorted_cereals[0],
        "most_caloric": sorted_cereals[-1],
    }


@pipeline
def inputs_pipeline():
    sort_by_calories(read_csv())


environment_dict = {
    "solids": {"read_csv": {"inputs": {"csv_path": {"value": "./data/cereal.csv"}}}}
}


result = execute_pipeline(inputs_pipeline, environment_dict=environment_dict)