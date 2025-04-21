# CSVBro

CSVBro is a command-line tool for viewing and analyzing CSV files using pandas DataFrames. It provides functionality to display specific columns, create pivot tables, output data in JSON format, and sort or limit the results.

## Author

- **Name**: Ryan Gerard Wilson
- **Website**: ryangerardwilson.com

## Features

- **Load and Display CSV**: Load a CSV file and display its contents as a pandas DataFrame.
- **Selective Column Display**: Show only specified columns from the DataFrame.
- **Pivot Tables**: Create pivot tables with aggregation functions like SUM, COUNT, COUNT_UNIQUE, MEAN, and MEDIAN.
- **JSON Output**: Export the DataFrame or selected columns to JSON format with pretty-printing.
- **Sorting and Limiting**: Sort data by a column in ascending or descending order and limit the number of rows displayed.
- **User-Friendly Interface**: Includes a loading animation and color-coded output (errors in red, data in blue, instructions in green).

## Requirements

- Python 3.6 or higher
- Required Python packages:
  - `pandas`
  - `numpy`

Install dependencies using:

```bash
pip install pandas numpy
```

## Installation

1. Clone or download the repository.
2. Ensure the required Python packages are installed (see Requirements).
3. Place the `main.py` and `ui.py` files in your project directory.

## Usage

Run the tool from the command line using:

```bash
python main.py <filename> [<command> [args]]
```

If no command is provided, the entire CSV is displayed as a DataFrame (with pandas' default truncation) unless `ORDER_BY` or `LIMIT` is specified.

### Commands

- **SHOW**: Display specified columns of the CSV.

  ```bash
  SHOW <column1> [<column2> ...] [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>]
  ```

- **PIVOT**: Create a pivot table with specified row, value, and aggregation function.

  ```bash
  PIVOT <row> <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>]
  ```

  - Aggregation functions: `SUM`, `COUNT`, `COUNT_UNIQUE`, `MEAN`, `MEDIAN`
  - Note: `SUM`, `MEAN`, and `MEDIAN` require numeric values.

- **JSON**: Output the DataFrame in JSON format.

  ```bash
  JSON [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>]
  ```

- **ORDER_BY**: Sort the entire DataFrame by a column.

  ```bash
  ORDER_BY <sort_column> <ASC|DESC> [LIMIT <n>]
  ```

- **LIMIT**: Limit the DataFrame to a specified number of rows.

  ```bash
  LIMIT <n>
  ```

### Examples

1. Display the entire CSV:

   ```bash
   python main.py data.csv
   ```

2. Show specific columns with sorting and limiting:

   ```bash
   python main.py data.csv SHOW account_id mobile ORDER_BY mobile ASC LIMIT 10
   ```

3. Create a pivot table with unique count:

   ```bash
   python main.py data.csv PIVOT lco_name mobile COUNT_UNIQUE ORDER_BY mobile DESC LIMIT 10
   ```

4. Output JSON with sorting and limiting:

   ```bash
   python main.py data.csv JSON ORDER_BY mobile DESC LIMIT 100
   ```

5. Sort and limit the entire DataFrame:

   ```bash
   python main.py data.csv ORDER_BY mobile ASC LIMIT 5
   ```

## Error Handling

- Invalid column names, aggregation functions, or sort directions will display an error message in red and exit.
- Non-numeric columns used with `SUM`, `MEAN`, or `MEDIAN` will result in an error.
- File not found or CSV loading errors will display an error and exit.

## Notes

- The tool uses threading to display a loading animation during CSV loading and pivot table creation.
- Output is color-coded: data and column names in blue, pivot table headers in green, errors in red.
- The `ui.py` module (not shown) is assumed to handle the UI-related functionality, such as colored output and animations.

## License

This project is licensed under the MIT License. See the LICENSE.md file for details.

