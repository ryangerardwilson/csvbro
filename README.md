# CSVBro

CSVBro is a command-line tool for viewing and analyzing CSV files using pandas DataFrames. It provides functionality to display specific columns, create pivot tables, output data in JSON format, sort, limit, and filter results.

## Author

- **Name**: Ryan Gerard Wilson
- **Website**: ryangerardwilson.com

## Features

- **Load and Display CSV**: Load a CSV file and display its contents as a pandas DataFrame.
- **Selective Column Display**: Show only specified columns from the DataFrame.
- **Pivot Tables**: Create pivot tables with aggregation functions like SUM, COUNT, COUNT_UNIQUE, MEAN, and MEDIAN.
- **JSON Output**: Export the DataFrame or selected columns to JSON format with pretty-printing.
- **Sorting and Limiting**: Sort data by a column in ascending or descending order and limit the number of rows displayed.
- **WHERE Clause Filtering**: Filter DataFrame rows based on conditions using operators `=`, `>`, `<`, `>=`, `<=`, `!=` for numeric and string comparisons, and `LIKE` for string containment. Supports complex conditions with `AND` and `OR` (e.g., `"Col1 > 0.3 AND (Col2 > 0.5 OR Col9 > 1.2)"`). Numeric comparisons use float parsing for accuracy. Conditions must be enclosed in double quotes in the shell command.
- **User-Friendly Interface**: Includes a loading animation and color-coded output (errors in red, data in blue, instructions in green).

## Installation

To install CSVBro, add the APT repository and install the package using the following command:

```bash
bash -c "sh <(curl -fsSL https://files.ryangerardwilson.com/csvbro/install.sh)"
```

This will:
1. Download and install the GPG key for the `csvbro` repository.
2. Add the `csvbro` repository to your APT sources list.
3. Update the APT cache and install the `csvbro` package.

After installation, the `csvbro` command is available system-wide.

## Usage

Run the tool from the command line using:

```bash
csvbro <filename> [<command> [args]]
```

If no command is provided, the entire CSV is displayed as a DataFrame (with pandas' default truncation) unless `ORDER_BY`, `LIMIT`, or `WHERE` is specified.

### Commands

- **SHOW**: Display specified columns of the CSV.

  ```bash
  SHOW <column1> [<column2> ...] [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE "<condition>"]
  ```

- **PIVOT**: Create a pivot table with specified row, value, and aggregation function.

  ```bash
  PIVOT <row> [<column>] <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE "<condition>"]
  ```

  - Aggregation functions: `SUM`, `COUNT`, `COUNT_UNIQUE`, `MEAN`, `MEDIAN`
  - Note: `SUM`, `MEAN`, and `MEDIAN` require numeric values.

- **PIVOT with DECILES**: Create a decile analysis of a column with 10 equal-width bins (D1 to D10).

  ```bash
  PIVOT DECILES(<column>[,IGNORE_OUTLIERS]) [<pivot_column>] <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE "<condition>"]
  ```

- **PIVOT with PERCENTILES**: Create a percentile analysis of a column with 10 equal-count bins (P0, P10, ..., P90).

  ```bash
  PIVOT PERCENTILES(<column>[,IGNORE_OUTLIERS]) [<pivot_column>] <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE "<condition>"]
  ```

- **JSON**: Output the DataFrame in JSON format.

  ```bash
  JSON [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE "<condition>"]
  ```

- **ORDER_BY**: Sort the entire DataFrame by a column.

  ```bash
  ORDER_BY <sort_column> <ASC|DESC> [LIMIT <n>] [WHERE "<condition>"]
  ```

- **LIMIT**: Limit the DataFrame to a specified number of rows.

  ```bash
  LIMIT <n> [WHERE "<condition>"]
  ```

- **WHERE**: Filter and display the DataFrame.

  ```bash
  WHERE "<condition>" [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>]
  ```

  - Condition syntax: `column <operator> value [AND|OR condition]` (e.g., `"column1 > 0.3"`, `"Col1 > 0.3 AND (Col2 > 0.5 OR Col9 > 1.2)"`)
  - Operators: `=`, `>`, `<`, `>=`, `<=`, `!=`, `LIKE` (for string containment)
  - Note: Numeric comparisons use float parsing; `LIKE` requires string columns and matches substrings (case-insensitive). Conditions must be enclosed in double quotes in the shell command.

### Examples

1. Display the entire CSV:

   ```bash
   csvbro data.csv
   ```

2. Show specific columns with sorting and limiting:

   ```bash
   csvbro data.csv SHOW account_id mobile ORDER_BY mobile ASC LIMIT 10
   ```

3. Create a pivot table with unique count:

   ```bash
   csvbro data.csv PIVOT lco_name category/ThirdParty mobile COUNT_UNIQUE ORDER_BY mobile DESC LIMIT 10
   ```

4. Create a decile analysis with a WHERE clause:

   ```bash
   csvbro data.csv PIVOT DECILES(splitter_efficacy_score,IGNORE_OUTLIERS) tenure_bin partner_id COUNT_UNIQUE WHERE "tenure_bin = '180+'"
   ```

5. Create a percentile analysis with sorting:

   ```bash
   csvbro data.csv PIVOT PERCENTILES(unique_splitter_contribution,IGNORE_OUTLIERS) partner_id COUNT_UNIQUE ORDER_BY count_unique DESC
   ```

6. Output JSON with sorting and limiting:

   ```bash
   csvbro data.csv JSON ORDER_BY mobile DESC LIMIT 100
   ```

7. Sort and limit the entire DataFrame with a WHERE clause:

   ```bash
   csvbro data.csv ORDER_BY mobile ASC LIMIT 5 WHERE "mobile = '1234567890'"
   ```

8. Filter and display the DataFrame with numeric comparison:

   ```bash
   csvbro data.csv WHERE "splitter_efficacy_score > 0.3"
   ```

9. Filter and display the DataFrame with string containment:

   ```bash
   csvbro data.csv WHERE "tenure_bin LIKE '180'"
   ```

10. Filter and display the DataFrame with complex condition:

    ```bash
    csvbro data.csv WHERE "splitter_efficacy_score > 0.3 AND (splitter_count > 100 OR active_base_customer_count < 50)"
    ```

## Error Handling

- Invalid column names, aggregation functions, sort directions, or WHERE conditions will display an error message in red and exit.
- Non-numeric columns used with `SUM`, `MEAN`, or `MEDIAN` will result in an error.
- File not found or CSV loading errors will display an error and exit.
- Type mismatches in WHERE clauses (e.g., comparing a numeric column to a string) will display an error.
- Using `LIKE` on non-string columns or invalid condition syntax will display an error.
- Insufficient unique values for `DECILES` or `PERCENTILES` (fewer than 10) will result in an error.

## Notes

- The tool uses threading to display a loading animation during CSV loading and pivot table creation.
- Output is color-coded: data and column names in blue, pivot table headers in green, errors in red.
- Debug messages are included to trace command parsing and execution.
- WHERE conditions must be enclosed in double quotes in the shell command to handle operators and complex logic correctly.
- `PERCENTILES` uses equal-count binning to ensure each bin contains approximately 10% of the data, with noise added to handle duplicate values.

## License

This project is licensed under the MIT License. See the LICENSE file for details.
