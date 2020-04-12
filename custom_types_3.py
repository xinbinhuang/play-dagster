import csv

from dagster import (
    DagsterType,
    Field,
    Selector,
    String,
    execute_pipeline,
    input_hydration_config,
    pipeline,
    solid,
)

from custom_types import less_simple_data_frame_type_check


@input_hydration_config(Selector({"csv": Field(String)}))
def less_simple_data_frame_input_hydration_config(context, selector):
    with open(selector["csv"], "r") as fd:
        lines = [row for row in csv.DictReader(fd)]

    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))
    return lines


LessSimpleDataFrameInputHydrated = DagsterType(
    name="LessSimpleDataFrame",
    description="A more sophisticated data frame that type checks its structure.",
    type_check_fn=less_simple_data_frame_type_check,
    input_hydration_config=less_simple_data_frame_input_hydration_config,
)


@solid
def sort_by_calories(context, cereals: LessSimpleDataFrameInputHydrated) -> None:
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
def custom_type_input_hydration_pipeline():
    sort_by_calories()


if __name__ == "__main__":

    execute_pipeline(
        custom_type_input_hydration_pipeline,
        {
            "solids": {
                "sort_by_calories": {"inputs": {"cereals": {"csv": "./data/cereal.csv"}}}
            }
        },
    )
