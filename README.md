# CSVBro

CSVBro is a command-line tool for viewing and analyzing CSV files using pandas DataFrames. It provides functionality to display specific columns, create pivot tables, output data in JSON format, sort, limit, and filter results.

- **Author**: Ryan Gerard Wilson
- **Website**: ryangerardwilson.com

## 1. Installation

To install `csvbro`, use pip:

    pip install csvbro

This installs `datasling` and its dependencies.

## 2. Get Latest Version

To upgrade to the latest version of `csvbro`:

    pip install --upgrade csvbro

## 3. Usage

Run the tool from the command line using:

    csvbro <filename> [<command> [args]]

If no command is provided, the entire CSV is displayed as a DataFrame (with pandas' default truncation) unless `ORDER_BY`, `LIMIT`, or `WHERE` is specified.

The following commands are available:

- **SHOW**: Display specified columns of the CSV.

    csvbro <filename> SHOW <column1> [<column2> ...] [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE "<condition>"]

- **PIVOT**: Create a pivot table with specified row, value, and aggregation function.

    csvbro <filename> PIVOT <row> [<column>] <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE "<condition>"]

  - Aggregation functions: `SUM`, `COUNT`, `COUNT_UNIQUE`, `MEAN`, `MEDIAN`
  - Note: `SUM`, `MEAN`, and `MEDIAN` require numeric values.

- **PIVOT with DECILES**: Create a decile analysis of a column with 10 equal-width bins (D1 to D10).

    csvbro <filename> PIVOT DECILES(<column>[,IGNORE_OUTLIERS]) [<pivot_column>] <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE "<condition>"]

- **PIVOT with PERCENTILES**: Create a percentile analysis of a column with 10 equal-count bins (P0, P10, ..., P90).

    csvbro <filename> PIVOT PERCENTILES(<column>[,IGNORE_OUTLIERS]) [<pivot_column>] <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE "<condition>"]

- **JSON**: Output the DataFrame in JSON format.

    csvbro <filename> JSON [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE "<condition>"]

- **ORDER_BY**: Sort the entire DataFrame by a column.

    csvbro <filename> ORDER_BY <sort_column> <ASC|DESC> [LIMIT <n>] [WHERE "<condition>"]

- **LIMIT**: Limit the DataFrame to a specified number of rows.

    csvbro <filename> [WHERE "<condition>"] LIMIT <n>

- **WHERE**: Filter and display the DataFrame.

    csvbro <filename> WHERE "<condition>" [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>]

  - Condition syntax: `column <operator> value [AND|OR condition]` (e.g., `"column1 > 0.3"`, `"Col1 > 0.3 AND (Col2 > 0.5 OR Col9 > 1.2)"`)
  - Operators: `=`, `>`, `<`, `>=`, `<=`, `!=`, `LIKE` (for string containment)
  - Note: Numeric comparisons use float parsing; `LIKE` requires string columns and matches substrings (case-insensitive). Conditions must be enclosed in double quotes in the shell command.

4. ## License

This project is licensed under the MIT License. See the LICENSE file for details.
