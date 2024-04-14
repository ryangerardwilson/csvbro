// csv_inspector.rs
use crate::user_experience::{
    handle_back_flag, handle_cancel_flag, handle_quit_flag, handle_special_flag,
};
use crate::user_interaction::{
    determine_action_as_number, get_edited_user_json_input, get_user_input_level_2,
    print_insight_level_2, print_list_level_2,
};
use rgwml::csv_utils::{CsvBuilder, Exp, ExpVal};
use serde_json::Value;

// Assuming CsvBuilder, Exp, and ExpVal are updated as per your implementation

struct ExpStore {
    expressions: Vec<Exp>, // Store the Exp instances directly
}

#[derive(Debug)]
enum CompareValue {
    Single(String),
    Multiple(Vec<String>),
}

impl ExpStore {
    fn add_expression(
        &mut self,
        column: String,
        operator: String,
        compare_value: CompareValue,
        compare_type: String,
    ) {
        let exp = match compare_value {
            CompareValue::Single(value) => Exp {
                column,
                operator,
                compare_with: ExpVal::STR(value),
                compare_as: compare_type,
            },
            CompareValue::Multiple(values) => Exp {
                column,
                operator,
                compare_with: ExpVal::VEC(values),
                compare_as: compare_type,
            },
        };

        self.expressions.push(exp);
    }

    fn get_exp(&self, index: usize) -> &Exp {
        &self.expressions[index]
    }
}

pub fn handle_inspect(
    csv_builder: &mut CsvBuilder,
    file_path_option: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    //pub async fn handle_inspect(csv_builder: &mut CsvBuilder) -> Result<(), Box<dyn std::error::Error>> {
    fn get_filter_expressions(
        data_store: &mut ExpStore,
    ) -> Result<(Vec<(String, usize)>, String), Box<dyn std::error::Error>> {
        let syntax = r#"{
  "expressions": [
    [
      "Exp1",
      {
        "column": "",
        "operator": "",
        "compare_with": "",
        "compare_as": ""
      }
    ]
  ],
  "evaluation": ""
}

SYNTAX
======

1. MULTIPLE EXPRESSION TEMPLATE
-------------------------------

{
  "expressions": [
    [
      "Exp1",
      {
        "column": "",
        "operator": "",
        "compare_with": "",
        "compare_as": ""
      }
    ],
    [
      "Exp2",
      {
        "column": "",
        "operator": "",
        "compare_with": "",
        "compare_as": ""
      }
    ]
  ],
  "evaluation": "Exp1 && Exp2"
}

2. NUMBERS/TIMESTAMPS/TEXT Operations
------------------------------------

### Expression

    {
      "expressions": [
        [
          "Exp1",
          {
            "column": "customer_type"
            "operator": "==",
            "compare_with": "PROSPECT",
            "compare_as": "TEXT"
          }
        ],
        [
          "Exp2",
          {
            "column": "added_at"
            "operator": ">",
            "compare_with": "2024-01-01 00:00:00",
            "compare_as": "TIMESTAMPS"
          }
        ],
        [,
          "Exp3",
          {
            "column": "invoice_amount"
            "operator": "<=",
            "compare_with": "5000",
            "compare_as": "NUMBERS"
          }
        ]
      ],
      "evaluation": "Exp1 && (Exp2 || Exp3)"
    }

### Available Operators

- NUMBERS/TIMESTAMPS (==, !=, >, <, >=, <=)
- TEXT (==, !=, CONTAINS, STARTS_WITH, DOES_NOT_CONTAIN)

3. VECTOR/ARRAY Operations
--------------------------

### Expression

    {
      "expressions": [
        [
          "Exp1",
          {
            "column": "address"
            "operator": "FUZZ_MIN_SCORE_60",
            "compare_with": [
                "public_school",
                "14 avenue",
              ],
            "compare_as": "TEXT"
          }
      ],
      "evaluation": "Exp1"
    }

### Available Operators

- FUZZ_MIN_SCORE_10/20/30, etc.

  "#;

        let exp_json = get_edited_user_json_input((&syntax).to_string());
        //dbg!(&exp_json);

        if handle_cancel_flag(&exp_json) {
            return Err("Operation canceled".into());
        }

        // Assume `last_exp_json` is a String containing your JSON data
        let parsed_json: Value = serde_json::from_str(&exp_json)?;

        let expressions = parsed_json["expressions"]
            .as_array()
            .ok_or("Invalid format for expressions")?;
        let mut expression_names = Vec::new();

        for (i, exp) in expressions.iter().enumerate() {
            let column = exp
                .get(1)
                .and_then(|col| col["column"].as_str())
                .ok_or("Invalid or missing column")?
                .to_string();

            let operator = exp
                .get(1)
                .and_then(|op| op["operator"].as_str())
                .ok_or("Invalid or missing operator")?
                .to_string();

            let compare_value = if let Some(compare_with_array) =
                exp.get(1).and_then(|cw| cw["compare_with"].as_array())
            {
                CompareValue::Multiple(
                    compare_with_array
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<String>>(), // Collecting as Vec<String>
                )
            } else if let Some(compare_with_single) =
                exp.get(1).and_then(|cw| cw["compare_with"].as_str())
            {
                CompareValue::Single(compare_with_single.to_string())
            } else {
                return Err("Invalid or missing compare_with".into());
            };

            let compare_type = if operator.starts_with("FUZZ_MIN_SCORE_") {
                "TEXT".to_string()
            } else {
                exp.get(1)
                    .and_then(|ct| ct["compare_as"].as_str())
                    .ok_or("Invalid or missing compare_as")?
                    .to_string()
            };

            // Add expressions to data store
            data_store.add_expression(column, operator, compare_value, compare_type);
            expression_names.push((format!("Exp{}", i + 1), i));
        }

        let result_expression = parsed_json["evaluation"]
            .as_str()
            .ok_or("Invalid or missing evaluation expression")?
            .to_string();

        Ok((expression_names, result_expression))
    }

    let menu_options = vec![
        "PRINT FIRST ROW",
        "PRINT LAST ROW",
        "PRINT ROWS (JSON)",
        "PRINT ALL ROWS (JSON)",
        "PRINT ALL ROWS (TABULATED)",
        "PRINT ROWS WHERE",
        "PRINT FREQ OF MULTIPLE COLUMN VALUES",
        "PRINT UNIQUE COLUMN VALUES",
        "PRINT COUNT WHERE",
        "PRINT DOT CHART (NORMAL)",
        "PRINT DOT CHART (CUMULATIVE)",
        "PRINT SMOOTH LINE CHART (NORMAL)",
        "PRINT SMOOTH LINE CHART (CUMULATIVE)",
    ];

    loop {
        print_insight_level_2("Select an option to inspect CSV data:");
        print_list_level_2(&menu_options);

        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        if handle_special_flag(&choice, csv_builder, file_path_option) {
            continue;
        }

        if handle_back_flag(&choice) {
            break;
        }
        let _ = handle_quit_flag(&choice);

        let selected_option = determine_action_as_number(&menu_options, &choice);

        match selected_option {
            Some(1) => {
                if choice.to_lowercase() == "1d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Prints the first row in JSON format.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|6  |books   |1000  |OTHER |2024-03-21|0                 |Y2024-M03       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
Total rows: 10

First row:
{
  "id": "1",
  "item": "books",
  "value": "1000",
  "type": "OTHER",
  "date": "2024-01-21",
  "relates_to_travel": "0",
  "date_YEAR_MONTH": "Y2024-M01",
}
"#,
                    );
                    continue;
                }

                csv_builder.print_first_row();
            }
            Some(2) => {
                if choice.to_lowercase() == "2d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Prints the last row in JSON format.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|6  |books   |1000  |OTHER |2024-03-21|0                 |Y2024-M03       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
Total rows: 10

Last row:
{
  "id": "10",
  "item": "movies",
  "value": "1500",
  "type": "OTHER",
  "date": "2024-01-25",
  "relates_to_travel": "0",
  "date_YEAR_MONTH": "Y2024-M01",
}
"#,
                    );
                    continue;
                }

                csv_builder.print_last_row();
            }
            Some(3) => {
                if choice.to_lowercase() == "3d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Prints a range of rows in JSON format.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|6  |books   |1000  |OTHER |2024-03-21|0                 |Y2024-M03       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
Total rows: 10

  @LILbro: Enter the start row number: 2
  @LILbro: Enter the end row number: 4

Row 2:
{
  "id": "2",
  "item": "snacks",
  "value": "200",
  "type": "FOOD",
  "date": "2024-02-22",
  "relates_to_travel": "0",
  "date_YEAR_MONTH": "Y2024-M02",
}
Row 3:
{
  "id": "3",
  "item": "cab fare",
  "value": "300",
  "type": "TRAVEL",
  "date": "2024-03-23",
  "relates_to_travel": "1",
  "date_YEAR_MONTH": "Y2024-M03",
}
Row 4:
{
  "id": "4",
  "item": "rent",
  "value": "20000",
  "type": "OTHER",
  "date": "2024-01-24",
  "relates_to_travel": "0",
  "date_YEAR_MONTH": "Y2024-M01",
}
"#,
                    );
                    continue;
                }

                let start_str = get_user_input_level_2("Enter the start row number: ");

                if handle_cancel_flag(&start_str) {
                    continue;
                }

                let start = start_str
                    .parse::<usize>()
                    .map_err(|_| "Invalid start row number")?;

                let end_str = get_user_input_level_2("Enter the end row number: ");

                if handle_cancel_flag(&end_str) {
                    continue;
                }

                let end = end_str
                    .parse::<usize>()
                    .map_err(|_| "Invalid start row number")?;

                csv_builder.print_rows_range(start, end);
            }

            Some(4) => {
                if choice.to_lowercase() == "4d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Prints all rows in JSON format.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|6  |books   |1000  |OTHER |2024-03-21|0                 |Y2024-M03       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
Total rows: 10

Row 1: 
{
  "id": "1",
  "item": "books",
  "value": "1000",
  "type": "OTHER",
  "date": "2024-01-21",
  "relates_to_travel": "0",
  "date_YEAR_MONTH": "Y2024-M01",
}
Row 2: 
{
  "id": "2",
  "item": "snacks",
  "value": "200",
  "type": "FOOD",
  "date": "2024-02-22",
  "relates_to_travel": "0",
  "date_YEAR_MONTH": "Y2024-M02",
}
.
.
.
Row 10: 
{
  "id": "10",
  "item": "movies",
  "value": "1500",
  "type": "OTHER",
  "date": "2024-01-25",
  "relates_to_travel": "0",
  "date_YEAR_MONTH": "Y2024-M01",
}

Total rows: 10
"#,
                    );
                    continue;
                }

                if csv_builder.has_data() {
                    csv_builder.print_rows();
                    println!();
                }
            }

            Some(5) => {
                if choice.to_lowercase() == "5d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Prints all rows in tabular format.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
<<+2 rows>>
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|11 |concert |2000  |OTHER |2024-03-27|0                 |Y2024-M03       |
|12 |alcohol |1100  |OTHER |2024-03-28|0                 |Y2024-M03       |
Total rows: 12

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|6  |books   |1000  |OTHER |2024-03-21|0                 |Y2024-M03       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|11 |concert |2000  |OTHER |2024-03-27|0                 |Y2024-M03       |
|12 |alcohol |1100  |OTHER |2024-03-28|0                 |Y2024-M03       |
Total rows: 12
"#,
                    );
                    continue;
                }

                if csv_builder.has_data() {
                    csv_builder.print_table_all_rows();
                    println!();
                }
            }

            Some(6) => {
                if choice.to_lowercase() == "6d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Prints all rows meeting specified conditions in JSON format.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
<<+2 rows>>
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|11 |concert |2000  |OTHER |2024-03-27|0                 |Y2024-M03       |
|12 |alcohol |1100  |OTHER |2024-03-28|0                 |Y2024-M03       |
Total rows: 12

  @LILbro: Executing this JSON query:
{
  "expressions": [
    [
      "Exp1",
      {
        "column": "value",
        "operator": "<",
        "compare_with": "1000",
        "compare_as": "NUMBERS"
      }
    ]
  ],
  "evaluation": "Exp1"
}

Row number: 2
{
  "id": "2",
  "item": "snacks",
  "value": "200",
  "type": "FOOD",
  "date": "2024-02-22",
  "relates_to_travel": "0",
  "date_YEAR_MONTH": "Y2024-M02",
}
Row number: 3
{
  "id": "3",
  "item": "cab fare",
  "value": "300",
  "type": "TRAVEL",
  "date": "2024-03-23",
  "relates_to_travel": "1",
  "date_YEAR_MONTH": "Y2024-M03",
}
Row number: 7
{
  "id": "7",
  "item": "snacks",
  "value": "200",
  "type": "FOOD",
  "date": "2024-01-22",
  "relates_to_travel": "0",
  "date_YEAR_MONTH": "Y2024-M01",
}
Row number: 8
{
  "id": "8",
  "item": "cab fare",
  "value": "300",
  "type": "TRAVEL",
  "date": "2024-02-23",
  "relates_to_travel": "1",
  "date_YEAR_MONTH": "Y2024-M02",
}
Total rows printed: 4
"#,
                    );
                    continue;
                }

                let mut exp_store = ExpStore {
                    expressions: Vec::new(),
                };

                match get_filter_expressions(&mut exp_store) {
                    Ok((expression_names, result_expression)) => {
                        let expressions_refs: Vec<(&str, Exp)> = expression_names
                            .iter()
                            .map(|(name, index)| (name.as_str(), exp_store.get_exp(*index).clone()))
                            .collect();
                        println!();
                        //dbg!(&expressions_refs, &result_expression);
                        csv_builder.print_rows_where(expressions_refs, &result_expression);
                    }
                    Err(e) if e.to_string() == "Operation canceled" => {
                        // If the operation was canceled by the user, do not print an error and just continue
                        continue;
                    }
                    Err(e) => {
                        println!("Error getting filter expressions: {}", e);
                        continue; // Return to the menu to let the user try again or choose another option
                    }
                }
            }
            Some(7) => {
                if choice.to_lowercase() == "7d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Prints frequencies of unique values in the specified columns.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
<<+2 rows>>
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|11 |concert |2000  |OTHER |2024-03-27|0                 |Y2024-M03       |
|12 |alcohol |1100  |OTHER |2024-03-28|0                 |Y2024-M03       |
Total rows: 12

  @LILbro: Enter column names separated by commas: item, value, type

Frequency for column 'item':
  alcohol: 1
  books: 2
  cab fare: 2
  concert: 1
  movies: 2
  rent: 2
  snacks: 2

Frequency for column 'value':
  1000: 2
  1100: 1
  1500: 2
  200: 2
  2000: 1
  20000: 2
  300: 2

Frequency for column 'type':
  FOOD: 2
  OTHER: 8
  TRAVEL: 2
"#,
                    );
                    continue;
                }

                let column_names =
                    get_user_input_level_2("Enter column names separated by commas: ");

                if handle_cancel_flag(&column_names) {
                    continue;
                }

                let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();
                csv_builder.print_freq(columns);
            }
            Some(8) => {
                if choice.to_lowercase() == "8d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Prints the unique values in the specified column.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
<<+2 rows>>
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|11 |concert |2000  |OTHER |2024-03-27|0                 |Y2024-M03       |
|12 |alcohol |1100  |OTHER |2024-03-28|0                 |Y2024-M03       |
Total rows: 12

  @LILbro: Enter the column name: value
Unique values in 'value': 200, 1000, 20000, 1500, 2000, 300, 1100
"#,
                    );
                    continue;
                }

                let column_name = get_user_input_level_2("Enter the column name: ");
                if handle_cancel_flag(&column_name) {
                    continue;
                }

                csv_builder.print_unique(&column_name.trim());
            }

            // In your handle_inspect method
            Some(9) => {
                if choice.to_lowercase() == "9d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Prints the unique values in the specified column.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
<<+2 rows>>
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|11 |concert |2000  |OTHER |2024-03-27|0                 |Y2024-M03       |
|12 |alcohol |1100  |OTHER |2024-03-28|0                 |Y2024-M03       |
Total rows: 12

  @LILbro: Executing this JSON query:
{
  "expressions": [
    [
      "Exp1",
      {
        "column": "value",
        "operator": "<",
        "compare_with": "1500",
        "compare_as": "NUMBERS"
      }
    ]
  ],
  "evaluation": "Exp1"
}

Count: 7
"#,
                    );
                    continue;
                }

                let mut exp_store = ExpStore {
                    expressions: Vec::new(),
                };

                match get_filter_expressions(&mut exp_store) {
                    Ok((expression_names, result_expression)) => {
                        let expressions_refs: Vec<(&str, Exp)> = expression_names
                            .iter()
                            .map(|(name, index)| (name.as_str(), exp_store.get_exp(*index).clone()))
                            .collect();
                        println!();
                        csv_builder.print_count_where(expressions_refs, &result_expression);
                    }
                    Err(e) if e.to_string() == "Operation canceled" => {
                        // If the operation was canceled by the user, do not print an error and just continue
                        continue;
                    }

                    Err(e) => {
                        println!("Error getting filter expressions: {}", e);
                        continue; // Return to the menu to let the user try again or choose another option
                    }
                }
            }
            Some(10) => {
                if choice.to_lowercase() == "10d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Plots two columns of a table in a dot-chart. In the event there are more than 80 values, the set of plots that best represent the curvature trend are chosen. 

TABLE
+++++

|id |value |date      |interest |
---------------------------------
|1  |500   |2024-04-08|7        |
|2  |450   |2024-04-07|8        |
|3  |420   |2024-04-06|9        |
|4  |400   |2024-04-05|7        |
|5  |380   |2024-04-05|7.2      |
|6  |360   |2024-04-03|8.2      |
|7  |340   |2024-04-02|9.2      |
|8  |320   |2024-04-01|7.4      |
|9  |300   |2024-04-08|8.4      |
|10 |280   |2024-04-08|9.4      |
Total rows: 10

  @LILbro: Enter the x-axis and y-axis column names separated by a comma: id, value

  |*                                                                              
  |                                                                               
  |                                                                               
  |                                                                               
  |                                                                               
  |        *                                                                      
  |                                                                               
  |                 *                                                             
  |                                                                               
  |                          *                                                    
  |                                  *                                            
  |                                                                               
  |                                           *                                   
  |                                                                               
  |                                                    *                          
  |                                                            *                  
  |                                                                               
  |                                                                     *         
  |                                                                              *
  +-------------------------------------------------------------------------------

  X-Axis Range: [1, 10]
  Y-Axis Range: [280, 500]
  X-Axis Min: 1
  X-Axis Max: 10
  Y-Axis Min: 280
  Y-Axis Max: 500
  Y-Axis Mean: 375.00
  Y-Axis Median: 370.00
"#,
                    );
                    continue;
                }

                let column_names = get_user_input_level_2(
                    "Enter the x-axis and y-axis column names (comma separated): ",
                );

                /*
                if column_names.to_lowercase() == "@cancel" {
                    continue;
                }
                */

                if handle_cancel_flag(&column_names) {
                    continue;
                }

                let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();

                // Ensuring exactly two columns were provided.
                if columns.len() != 2 {
                    // Handle the error: inform the user they need to enter exactly two column names.
                    print_insight_level_2(
                        "Please enter exactly two column names, separated by a comma.",
                    );
                    continue;
                } else {
                    // Extracting the column names.
                    let x_axis_column = columns[0];
                    let y_axis_column = columns[1];
                    println!();
                    // Using the columns in your function.
                    csv_builder.print_dot_chart(x_axis_column, y_axis_column);
                }
            }
            Some(11) => {
                if choice.to_lowercase() == "11d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Plots two columns of a table in a dot-chart. In the event there are more than 80 values, the set of plots that best represent the curvature trend are chosen. 

TABLE
+++++

|id |value |date      |interest |
---------------------------------
|1  |500   |2024-04-08|7        |
|2  |450   |2024-04-07|8        |
|3  |420   |2024-04-06|9        |
|4  |400   |2024-04-05|7        |
|5  |380   |2024-04-05|7.2      |
|6  |360   |2024-04-03|8.2      |
|7  |340   |2024-04-02|9.2      |
|8  |320   |2024-04-01|7.4      |
|9  |300   |2024-04-08|8.4      |
|10 |280   |2024-04-08|9.4      |
Total rows: 10

  @LILbro: Enter the x-axis and y-axis column names separated by a comma: id, value

  |                                                                              *
  |
  |                                                                     *
  |                                                            *
  |
  |                                                    *
  |                                           *
  |
  |                                  *
  |
  |                          *
  |
  |                 *
  |
  |        *
  |
  |*
  |
  |
  +-------------------------------------------------------------------------------

  X-Axis Range: [1, 10]
  Lowest Non-Zero Cumulative Y-Axis Value: 500
  Cumulative Y-Axis Max: 3750
"#,
                    );
                    continue;
                }

                let column_names = get_user_input_level_2(
                    "Enter the x-axis and y-axis column names (comma separated): ",
                );

                /*
                if column_names.to_lowercase() == "@cancel" {
                    continue;
                }
                */

                if handle_cancel_flag(&column_names) {
                    continue;
                }

                let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();

                // Ensuring exactly two columns were provided.
                if columns.len() != 2 {
                    // Handle the error: inform the user they need to enter exactly two column names.
                    print_insight_level_2(
                        "Please enter exactly two column names, separated by a comma.",
                    );
                    continue;
                } else {
                    // Extracting the column names.
                    let x_axis_column = columns[0];
                    let y_axis_column = columns[1];
                    println!();
                    // Using the columns in your function.
                    csv_builder.print_cumulative_dot_chart(x_axis_column, y_axis_column);
                }
            }

            Some(12) => {
                if choice.to_lowercase() == "12d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Plots two columns of a table in a smooth line-chart, upon analyzing plot points, that best represent the curvature trajectory. 

TABLE
+++++

|id |value |date      |interest |
---------------------------------
|1  |500   |2024-04-08|7        |
|2  |450   |2024-04-07|8        |
|3  |420   |2024-04-06|9        |
|4  |400   |2024-04-05|7        |
|5  |380   |2024-04-05|7.2      |
|6  |360   |2024-04-03|8.2      |
|7  |340   |2024-04-02|9.2      |
|8  |320   |2024-04-01|7.4      |
|9  |300   |2024-04-08|8.4      |
|10 |280   |2024-04-08|9.4      |
Total rows: 10

  @LILbro: Enter the x-axis and y-axis column names separated by a comma: id, value

  |**
  |  **
  |    **
  |      **
  |        ***
  |           ***
  |              ****
  |                  *****
  |                       *****
  |                            *****
  |                                 ******
  |                                       *****
  |                                            *****
  |                                                 ******
  |                                                       *****
  |                                                            *****
  |                                                                 ******
  |                                                                       *****
  |                                                                            ***
  +-------------------------------------------------------------------------------

  X-Axis Range: [1, 10]
  Y-Axis Range: [280, 500]
  X-Axis Min: 1
  X-Axis Max: 10
  Y-Axis Min: 280
  Y-Axis Max: 500
  Y-Axis Mean: 375.00
  Y-Axis Median: 370.00
"#,
                    );
                    continue;
                }

                let column_names = get_user_input_level_2(
                    "Enter the x-axis and y-axis column names (comma separated): ",
                );

                /*
                if column_names.to_lowercase() == "@cancel" {
                    continue;
                }
                */

                if handle_cancel_flag(&column_names) {
                    continue;
                }

                let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();

                // Ensuring exactly two columns were provided.
                if columns.len() != 2 {
                    // Handle the error: inform the user they need to enter exactly two column names.
                    print_insight_level_2(
                        "Please enter exactly two column names, separated by a comma.",
                    );
                    continue;
                } else {
                    // Extracting the column names.
                    let x_axis_column = columns[0];
                    let y_axis_column = columns[1];
                    println!();
                    // Using the columns in your function.
                    csv_builder.print_smooth_line_chart(x_axis_column, y_axis_column);
                }
            }

            Some(13) => {
                if choice.to_lowercase() == "13d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Plots two columns of a table in a smooth line-chart, upon analyzing plot points, that best represent the curvature trajectory. 

TABLE
+++++

|id |value |date      |interest |
---------------------------------
|1  |500   |2024-04-08|7        |
|2  |450   |2024-04-07|8        |
|3  |420   |2024-04-06|9        |
|4  |400   |2024-04-05|7        |
|5  |380   |2024-04-05|7.2      |
|6  |360   |2024-04-03|8.2      |
|7  |340   |2024-04-02|9.2      |
|8  |320   |2024-04-01|7.4      |
|9  |300   |2024-04-08|8.4      |
|10 |280   |2024-04-08|9.4      |
Total rows: 10

  @LILbro: Enter the x-axis and y-axis column names separated by a comma: id, value

  |                                                                           ****
  |                                                                     ******    
  |                                                               ******          
  |                                                         ******                
  |                                                    *****                      
  |                                              ******                           
  |                                         *****                                 
  |                                    *****                                      
  |                               *****                                           
  |                           ****                                                
  |                      *****                                                    
  |                  ****                                                         
  |             *****                                                             
  |         ****                                                                  
  |     ****                                                                      
  | ****                                                                          
  |*                                                                              
  |                                                                               
  |                                                                               
  +-------------------------------------------------------------------------------

  X-Axis Range: [1, 10]
  Lowest Non-Zero Cumulative Y-Axis Value: 500
  Cumulative Y-Axis Max: 3750
"#,
                    );
                    continue;
                }

                let column_names = get_user_input_level_2(
                    "Enter the x-axis and y-axis column names (comma separated): ",
                );

                /*
                if column_names.to_lowercase() == "@cancel" {
                    continue;
                }
                */

                if handle_cancel_flag(&column_names) {
                    continue;
                }

                let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();

                // Ensuring exactly two columns were provided.
                if columns.len() != 2 {
                    // Handle the error: inform the user they need to enter exactly two column names.
                    print_insight_level_2(
                        "Please enter exactly two column names, separated by a comma.",
                    );
                    continue;
                } else {
                    // Extracting the column names.
                    let x_axis_column = columns[0];
                    let y_axis_column = columns[1];
                    println!();
                    // Using the columns in your function.
                    csv_builder.print_cumulative_smooth_line_chart(x_axis_column, y_axis_column);
                }
            }

            _ => {
                println!("Invalid option. Please enter a number from 1 to 13.");
                continue; // Ask for the choice again
            }
        }

        println!(); // Print a new line for better readability
    }

    Ok(())
}
