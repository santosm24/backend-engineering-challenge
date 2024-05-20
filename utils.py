import pandas as pd
import pandera as pa
import json

def validate_and_flag(row, schema):
    """
    Validate a row against a given schema and return a boolean flag.

    Parameters:
    row (pd.Series): The row of the DataFrame to validate.
    schema (pa.DataFrameSchema): The schema to validate against.

    Returns:
    bool: True if the row is valid, False otherwise.
    """
    try:
        schema.validate(pd.DataFrame([row]))
        return True
    except pa.errors.SchemaError as err:
        print(err)
        return False


def read_file(input_file, schema):
    """
    Read a JSON file, validate its rows against a given schema, and return a DataFrame of valid rows.

    Parameters:
    input_file (str): The path to the input JSON file.
    schema (pa.DataFrameSchema): The schema to validate against.

    Returns:
    pd.DataFrame: DataFrame containing only the valid rows according to the schema.
    """
    df = pd.read_json(input_file, lines=True,)

    df['validate']=df.apply(validate_and_flag, axis=1, schema=schema)

    df = df[df['validate']].drop(columns=['validate'])
    
    df = df[schema.columns.keys()]

    df = schema.validate(df)

    return df

def write_json_file(file_path, elements):
    with open(file_path, 'w+') as file:
        for element in elements:
            json.dump(element, file)
            file.write('\n')