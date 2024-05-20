## 1. Prerequisites

- Python 3.x installed on your system. You can download it from [here](https://www.python.org/downloads/).

## 2. Set Up Virtual Environment

```bash
# Create a virtual environment named 'unbabel'
python -m venv unbabel

# Activate the virtual environment
# On Windows
source unbabel\Scripts\activate
# On macOS/Linux
source unbabel/bin/activate
```

## 3. Install Required Libraries

```bash
# Install required libraries from requirements.txt
pip install -r requirements.txt
```

## 4. Run the Unbabel Script

```bash
# Ensure you are in the virtual environment and in the project directory

# Run the Unbabel script with input file and window size parameters
python unbabel_cli.py --input_file input.json --window_size 10
```

## 5. Running Tests

```bash
# Ensure you are in the virtual environment and in the project directory

# Run tests using pytest
pytest test.py
```

## Considerations

For this exercise, I use pandas to process the data and use only the columns that are important for the exercise to improve the performance if we are working with a huge amount of data. Another option that can be a good fit for this project is to use Spark, but that will be much harder to properly set up and install in a short amount of time.