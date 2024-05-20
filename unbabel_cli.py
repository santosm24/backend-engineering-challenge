import pandas as pd
import pandera as pa
import argparse
from datetime import timedelta


from utils import read_file, write_json_file

# Define your schema
schema = pa.DataFrameSchema(
    {
        "timestamp": pa.Column(pa.DateTime),
        "duration": pa.Column(pa.Int),
    },
    coerce=True,
)


def moving_window(df, window):
    """
    Computes the moving window average delivery time for a DataFrame.

    Args:
        df (pandas.DataFrame): DataFrame containing at least 'timestamp' and 'duration' columns.
        window (int): Size of the window in minutes.

    Returns:
        list: List of dictionaries containing the date and the average delivery time for each window.
    """
    start_window = df["timestamp"].min().floor("min") - timedelta(minutes=window)
    max_time = df["timestamp"].max().ceil("min")
    result = []
    while start_window < max_time:
        end_window = start_window + timedelta(minutes=window)
        window_range = df[
            (df["timestamp"] > start_window) & (df["timestamp"] < end_window)
        ]

        avg_duration = window_range["duration"].mean()
        result.append(
            {
                "date": end_window.strftime('%Y-%m-%d %H:%M:%S'),
                "average_delivery_time": round(avg_duration,1) if not pd.isna(avg_duration) else 0.0,
            }
        )
        start_window = start_window + timedelta(minutes=1)
    
    return result


def main():

    parser = argparse.ArgumentParser(description="Insert input_file and window_size")
    parser.add_argument("--input_file", type=str, help="Path to the input file")
    parser.add_argument("--window_size", type=int, help="Size of the window")

    args = parser.parse_args()

    df = read_file(args.input_file, schema)

    output = moving_window(df, args.window_size)

    write_json_file('output.txt', output)


if __name__ == "__main__":
    main()
