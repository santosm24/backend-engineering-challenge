import pandas as pd
import pandera as pa
import pytest
import tempfile
import json

from utils import validate_and_flag, read_file
from unbabel_cli import moving_window

# Define the schema
schema = pa.DataFrameSchema(
    {
        "timestamp": pa.Column(pa.DateTime),
        "duration": pa.Column(pa.Int),
    },
    coerce=True,
)

valid_data = {
    "timestamp": "2018-12-26 18:12:19.903159",
    "translation_id": "5aa5b2f39f7254a75aa4",
    "source_language": "en",
    "target_language": "fr",
    "client_name": "airliberty",
    "event_name": "translation_delivered",
    "duration": 20,
    "nr_words": 100,
}

invalid_data2 = {
    "timestamp": "2018-12-26 18:12:19.903159",
    "translation_id": "5aa5b2f39f7254a75aa4",
    "source_language": "en",
    "target_language": "fr",
    "client_name": "airliberty",
    "event_name": "translation_delivered",
    "duration": "20",
    "nr_words": 100,
}


invalid_data = {
    "timestamp": "2018-12-26 18:12:19.903159",
    "translation_id": "5aa5b2f39f7254a75aa4",
    "source_language": "en",
    "target_language": "fr",
    "client_name": "airliberty",
    "event_name": "translation_delivered",
    "duration": "invalid",  # Invalid duration type
    "nr_words": 100,
}


def test_validate_and_flag():
    # Valid row
    assert validate_and_flag(pd.Series(valid_data), schema) == True

    # Valid row but with duration as string
    assert validate_and_flag(pd.Series(invalid_data2), schema) == True

    # Invalid row with invalid duration
    assert validate_and_flag(pd.Series(invalid_data), schema) == False


def test_read_file():
    # Create a temporary JSON file
    with tempfile.NamedTemporaryFile(
        mode="w", delete=False, suffix=".json"
    ) as temp_file:
        temp_file.write(json.dumps(valid_data) + "\n")
        temp_file.write(json.dumps(invalid_data) + "\n")
        temp_file_path = temp_file.name

    # Read and validate the file
    valid_df = read_file(temp_file_path, schema)

    # Expected DataFrame
    expected_df = pd.DataFrame([valid_data])[["timestamp", "duration"]]

    expected_df = schema.validate(expected_df)

    # Assert that the valid DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(valid_df, expected_df)


# Example DataFrame
df_moving_window = pd.DataFrame(
    {
        "timestamp": pd.date_range(start="2022-01-01 00:00:00", periods=10, freq="min"),
        "duration": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    }
)

expected_result_moving_window = [
    {"date": "2022-01-01 00:00:00", "average_delivery_time": 0.0},
    {"date": "2022-01-01 00:01:00", "average_delivery_time": 10.0},
    {"date": "2022-01-01 00:02:00", "average_delivery_time": 15.0},
    {"date": "2022-01-01 00:03:00", "average_delivery_time": 20.0},
    {"date": "2022-01-01 00:04:00", "average_delivery_time": 25.0},
    {"date": "2022-01-01 00:05:00", "average_delivery_time": 35.0},
    {"date": "2022-01-01 00:06:00", "average_delivery_time": 45.0},
    {"date": "2022-01-01 00:07:00", "average_delivery_time": 55.0},
    {"date": "2022-01-01 00:08:00", "average_delivery_time": 65.0},
    {"date": "2022-01-01 00:09:00", "average_delivery_time": 75.0},
    {"date": "2022-01-01 00:10:00", "average_delivery_time": 85.0},
    {"date": "2022-01-01 00:11:00", "average_delivery_time": 90.0},
    {"date": "2022-01-01 00:12:00", "average_delivery_time": 95.0},
    {"date": "2022-01-01 00:13:00", "average_delivery_time": 100.0},
]


def test_moving_window():

    # Call the function
    result = moving_window(df_moving_window, window=5)

    # Check if the result is a list
    assert isinstance(result, list)

    # Check if each entry in the result is a dictionary with keys 'date' and 'average_delivery_time'
    for entry in result:
        assert isinstance(entry, dict)
        assert "date" in entry
        assert "average_delivery_time" in entry

    # Check if the length of the result matches the expected length
    expected_length = (
        len(df_moving_window["timestamp"]) + 4
    )  # Length of df plus window size minus 1
    assert len(result) == expected_length

    # Check if the result is the expected
    assert result == expected_result_moving_window


if __name__ == "__main__":
    pytest.main()
