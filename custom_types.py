import csv

from dagster import DagsterType, pipeline, solid

# 1. simple DataFrame
SimpleDataFrame = DagsterType(
    name="SimpleDataFrame",
    type_check_fn=lambda _, value: isinstance(value, list),
    description="A naive representation of a data frame, e.g., as returned by csv.DictReader.",
)


# 2. less simple DataFrame
def less_simple_data_frame_type_check(_, value):
    if not isinstance(value, list):
        return False

    fields = [field for field in value[0].keys()]

    for row in value:
        if not isinstance(row, dict):
            return False
        row_fields = [field for field in row.keys()]
        if fields != row_fields:
            return False

    return True


LessSimpleDataFrame = DagsterType(
    name="LessSimpleDataFrame",
    description="A more sophisticated data frame that type checks its structure.",
    type_check_fn=less_simple_data_frame_type_check,
)


@solid
def read_csv(context, csv_path: str) -> LessSimpleDataFrame:
    with open(csv_path, "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))
    return lines


@solid
def sort_by_calories(context, cereals: LessSimpleDataFrame):
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


@pipeline
def custom_type_pipeline():
    sort_by_calories(read_csv())
