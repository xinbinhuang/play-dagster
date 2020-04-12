from dagster import check_dagster_type

from custom_types_3 import LessSimpleDataFrameInputHydrated


def test_less_simple_data_frame():
    assert check_dagster_type(
        LessSimpleDataFrameInputHydrated, [{'foo': 1}, {'foo': 2}]
    ).success

    type_check = check_dagster_type(
        LessSimpleDataFrameInputHydrated, [{'foo': 1}, {'bar': 2}]
    )
    assert not type_check.success
