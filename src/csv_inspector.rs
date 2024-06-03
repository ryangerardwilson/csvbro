// csv_inspector.rs
use crate::user_experience::handle_cancel_flag;
use crate::user_interaction::{
    get_edited_user_json_input, get_user_input_level_2, print_insight_level_2, print_list_level_2,
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

pub async fn handle_inspect(
    mut csv_builder: CsvBuilder,
    _file_path_option: Option<&str>,
    action_type: &str,
    action_feature: &str,
    action_flag: &str,
    action_menu_options: Vec<&str>,
) -> Result<(CsvBuilder, bool), Box<dyn std::error::Error>> {
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

    match action_feature {
        "" => {
            let action_sub_menu_options = vec![
                "PRINT FIRST ROW",
                "PRINT LAST ROW",
                "PRINT ROWS (JSON)",
                "PRINT ALL ROWS (JSON)",
                "PRINT ALL ROWS (TABULATED)",
                "PRINT CLEANLINESS REPORT",
                "PRINT ROWS WHERE",
                "PRINT NUMERICAL ANALYSIS",
                "PRINT FREQ OF MULTIPLE COLUMN VALUES (LINEAR)",
                "PRINT FREQ OF MULTIPLE COLUMN VALUES (CASCADING)",
                "PRINT UNIQUE COLUMN VALUES",
                "PRINT STATS OF UNIQUE VALUE FREQ",
                "PRINT COUNT WHERE",
                "PRINT DOT CHART (NORMAL)",
                "PRINT DOT CHART (CUMULATIVE)",
                "PRINT SMOOTH LINE CHART (NORMAL)",
                "PRINT SMOOTH LINE CHART (CUMULATIVE)",
                "PRINT CLEANLINESS REPORT BY COLUMN PARSE",
            ];

            print_list_level_2(&action_menu_options, &action_sub_menu_options, &action_type);
            return Ok((csv_builder, false));
        }

        "1" => {
            if action_flag == "d" {
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
                return Ok((csv_builder, false));
            }

            csv_builder.print_first_row();
        }
        "2" => {
            if action_flag == "d" {
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
                return Ok((csv_builder, false));
            }

            csv_builder.print_last_row();
        }
        "3" => {
            if action_flag == "d" {
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
                return Ok((csv_builder, false));
            }

            let start_str = get_user_input_level_2("Enter the start row number: ");

            if handle_cancel_flag(&start_str) {
                return Ok((csv_builder, false));
            }

            let start = start_str
                .parse::<usize>()
                .map_err(|_| "Invalid start row number")?;

            let end_str = get_user_input_level_2("Enter the end row number: ");

            if handle_cancel_flag(&end_str) {
                return Ok((csv_builder, false));
            }

            let end = end_str
                .parse::<usize>()
                .map_err(|_| "Invalid start row number")?;

            csv_builder.print_rows_range(start, end);
        }

        "4" => {
            if action_flag == "d" {
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
                return Ok((csv_builder, false));
            }

            if csv_builder.has_data() {
                csv_builder.print_rows();
                println!();
            }
        }

        "5" => {
            if action_flag == "d" {
                // if choice.to_lowercase() == "5d" {
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
                return Ok((csv_builder, false));
            }

            if csv_builder.has_data() {
                csv_builder.print_table_all_rows();
                println!();
            }
        }

        "6" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

// Prints the scope to cleans data by parsing columns with preset rules.
  @LILbro: Executing this JSON query:
{
    "mobile": ["HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER", "HAS_LENGTH:10"],
    "price": [],
    "paid_on": ["IS_DATETIME_PARSEABLE"],
}

### AVAILABLE RULES

- "HAS_ONLY_NUMERICAL_VALUES"
- "HAS_ONLY_POSITIVE_NUMERICAL_VALUES"
- "HAS_LENGTH:10"
- "HAS_MIN_LENGTH:7"
- "HAS_MAX_LENGTH:12"
- "HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER"
- "HAS_NO_EMPTY_STRINGS"
- "IS_DATETIME_PARSEABLE"
"#,
                );
                return Ok((csv_builder, false));
            }

            if let Some(headers) = csv_builder.get_headers() {
                let mut json_array_str = "{\n".to_string();

                // Loop through headers and append them as keys in the JSON array string, excluding auto-computed columns
                for (i, header) in headers.iter().enumerate() {
                    if header != "id" && header != "c@" && header != "u@" {
                        json_array_str.push_str(&format!("    \"{}\": []", header));
                        if i < headers.len() - 1 {
                            json_array_str.push_str(",\n");
                        }
                    }
                }

                // Close the first JSON object and start the syntax explanation
                json_array_str.push_str("\n}");

                let syntax_explanation = r#"

SYNTAX
======

### Example

{
  "column1": ["HAS_ONLY_POSITIVE_NUMERICAL_VALUES", "HAS_NO_EMPTY_STRINGS"],
  "column2": [],
  "column3": ["HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER"],
  "column4": [],
  "column5": [],
  "column6": ["IS_DATETIME_PARSEABLE"],
  "column7": ["IS_DATETIME_PARSEABLE"]
}

### AVAILABLE RULES
- "HAS_ONLY_NUMERICAL_VALUES"
- "HAS_ONLY_POSITIVE_NUMERICAL_VALUES"
- "HAS_LENGTH:10"
- "HAS_MIN_LENGTH:7"
- "HAS_MAX_LENGTH:12"
- "HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER"
- "HAS_NO_EMPTY_STRINGS"
- "IS_DATETIME_PARSEABLE"
"#;

                let full_syntax = json_array_str + syntax_explanation;

                // Get user input
                let rows_json_str = get_edited_user_json_input(full_syntax);
                //dbg!(&rows_json_str);
                if handle_cancel_flag(&rows_json_str) {
                    return Ok((csv_builder, false));
                }

                // Parse the user input
                let rows_json: Value = match serde_json::from_str(&rows_json_str) {
                    Ok(json) => json,
                    Err(e) => {
                        eprintln!("Error parsing JSON string: {}", e);
                        return Ok((csv_builder, false));
                    }
                };

                // Collect rules from user input
                let mut rules = Vec::new();
                if let Some(obj) = rows_json.as_object() {
                    for (key, value) in obj {
                        if let Some(rules_array) = value.as_array() {
                            let mut column_rules = Vec::new();
                            for rule in rules_array {
                                if let Some(rule_str) = rule.as_str() {
                                    if !rule_str.is_empty() {
                                        column_rules.push(rule_str.to_string());
                                    }
                                }
                            }
                            if !column_rules.is_empty() {
                                rules.push((key.clone(), column_rules));
                            }
                        }
                    }
                }

                println!();
                // Invoke the cleanliness report function with the collected rules
                csv_builder.print_cleanliness_report_by_column_parse(rules);
            }
        }

        "7" => {
            if action_flag == "d" {
                // if choice.to_lowercase() == "7d" {
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
                return Ok((csv_builder, false));
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
                    return Ok((csv_builder, false));
                }
                Err(e) => {
                    println!("Error getting filter expressions: {}", e);
                    return Ok((csv_builder, false));
                }
            }
        }
        "8" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

  @LILbro: Enter column names separated by commas: price, gst

Analysis for column 'price':
  Minimum: -0.8474576271186441
  Maximum: 995.7627118644068
  Range: 5519.49
  Sum: 68325587.29
  Mean: 441.36
  Median: 392.37
  Mode: 380.5084745762712
  Standard Deviation: 207.10
  Variance: 42890.76
  Sum of Squared Deviations: 6639789909.14
Analysis for column 'gst':
  Minimum: -0.1525423728813559
  Maximum: 99.7627118644068
  Range: 993.51
  Sum: 12298605.71
  Mean: 79.44
  Median: 70.63
  Mode: 68.49152542372882
  Standard Deviation: 37.28
  Variance: 1389.66
  Sum of Squared Deviations: 215129193.06

### DEFINITIONS

  - Minimum (Smallest Number): Imagine you’re looking for the smallest shell on the beach. A small value here means it's really tiny, maybe as small as a grain of sand!

  - Maximum (Biggest Number): Now think of finding the biggest shell. A big value here means it’s huge, maybe even as big as your hand!

  - Range (Difference between Biggest and Smallest): This is like measuring the space between two trees in the playground. A large range means the trees are really far apart. A small range means they are close together, so you don't have to walk far!

  - Sum (All Numbers Added Together): This is like collecting stones. A large sum means you’ve gathered lots of stones, maybe enough to fill your pockets! A small sum means just a few stones, maybe only enough to hold in one hand.

  - Mean (Average): Think about sharing cookies equally with friends. A large average means each friend gets many cookies, maybe a whole bunch! A small average means each friend gets maybe only one or a tiny piece.

  - Median (Middle Number in a Sorted List): This is the middle step on a staircase. If it’s a high step (large median), it means you're up high, like on a big slide. If it’s a low step (small median), you're closer to the ground, like sitting in a sandbox.

  - Mode (Most Frequent Number): Imagine which color balloon you see the most at a party. A large mode means that most kids chose a big balloon. A small mode might mean most kids picked a small balloon.

  - Standard Deviation (How Spread Out the Numbers Are): This is like seeing how spread out friends are in a game of hide and seek. A large deviation means everyone is hiding far away from each other. A small deviation means everyone is hiding close by, maybe even in the same spot! Compare the standard deviation to the mean itself. For instance, if the SD is very large compared to the mean, it indicates high variability.

  - Variance (Average of Squared Differences from the Mean): This is also about how spread out things are, like different sizes of pumpkins in a patch. A large variance means pumpkins range from tiny to huge. A small variance means most pumpkins are about the same size. Like standard deviation, variance can be useful when viewed relative to the mean. If variance is very high compared to the mean, it indicates high variability.

  - Sum of Squared Deviations (Total of Each Value's Difference from the Mean, Squared): Think of it like the total jumps needed to reach different distances in hopscotch. A large total means some jumps were really big. A small total means the jumps were mostly the same, easy hops. The sum of squared deviations (SSD), or sum of squares, measures the total variability in a dataset. However, it isn't interpreted directly because its value depends on the number of data points, making comparisons challenging across different datasets. Instead, it is often used as an intermediate calculation for other metrics like variance or standard deviation. 
"#,
                );
                return Ok((csv_builder, false));
            }

            let column_names = get_user_input_level_2("Enter column names separated by commas: ");

            if handle_cancel_flag(&column_names) {
                return Ok((csv_builder, false));
            }

            let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();
            println!();
            csv_builder.print_column_numerical_analysis(columns);
        }

        "9" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Prints frequencies of unique values in the specified columns.
|id |value |date      |interest |type  |
----------------------------------------
|1  |500   |2024-04-08|7        |FOOD  |
|2  |450   |2024-04-07|8        |FOOD  |
|3  |420   |2024-04-06|9        |TRAVEL|
|4  |400   |2024-04-05|7        |OTHER |
|5  |380   |2024-04-05|7.2      |TRAVEL|
|6  |360   |2024-04-03|8.2      |OTHER |
|7  |340   |2024-04-02|9.2      |FOOD  |
|8  |320   |2024-04-01|7.4      |TRAVEL|
|9  |300   |2024-04-08|8.4      |FOOD  |
|10 |280   |2024-04-08|9.4      |FOOD  |
Total rows: 10

  @LILbro: Enter column names separated by commas: type, interest

Frequency for column 'type':
  OTHER : f = 2 (20%)
  FOOD  : f = 5 (50%)
  TRAVEL: f = 3 (30%)

Frequency for column 'interest':
  7  : f = 2 (20%)
  7.2: f = 1 (10%)
  7.4: f = 1 (10%)
  8  : f = 1 (10%)
  8.2: f = 1 (10%)
  8.4: f = 1 (10%)
  9  : f = 1 (10%)
  9.2: f = 1 (10%)
  9.4: f = 1 (10%)
"#,
                );
                return Ok((csv_builder, false));
            }

            let column_names = get_user_input_level_2("Enter column names separated by commas: ");

            if handle_cancel_flag(&column_names) {
                return Ok((csv_builder, false));
            }

            let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();
            csv_builder.print_freq(columns);
        }
        "10" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Prints cascading frequency tables for selected columns of a dataset.
|id |value |date      |interest |type  |
----------------------------------------
|1  |500   |2024-04-08|7        |FOOD  |
|2  |450   |2024-04-07|8        |FOOD  |
|3  |420   |2024-04-06|9        |TRAVEL|
|4  |400   |2024-04-05|7        |OTHER |
|5  |380   |2024-04-05|7.2      |TRAVEL|
|6  |360   |2024-04-03|8.2      |OTHER |
|7  |340   |2024-04-02|9.2      |FOOD  |
|8  |320   |2024-04-01|7.4      |TRAVEL|
|9  |300   |2024-04-08|8.4      |FOOD  |
|10 |280   |2024-04-08|9.4      |FOOD  |
Total rows: 10

  @LILbro: Enter column names separated by commas: type, interest, value

Frequency for column 'type':
  OTHER : f = 2 (20.00%)
    Frequency for column 'interest':
      7  : f = 1 (50.00%)
        Frequency for column 'value':
          400: f = 1 (100.00%)
      8.2: f = 1 (50.00%)
        Frequency for column 'value':
          360: f = 1 (100.00%)
  FOOD  : f = 5 (50.00%)
    Frequency for column 'interest':
      7  : f = 1 (20.00%)
        Frequency for column 'value':
          500: f = 1 (100.00%)
      8  : f = 1 (20.00%)
        Frequency for column 'value':
          450: f = 1 (100.00%)
      8.4: f = 1 (20.00%)
        Frequency for column 'value':
          300: f = 1 (100.00%)
      9.2: f = 1 (20.00%)
        Frequency for column 'value':
          340: f = 1 (100.00%)
      9.4: f = 1 (20.00%)
        Frequency for column 'value':
          280: f = 1 (100.00%)
  TRAVEL: f = 3 (30.00%)
    Frequency for column 'interest':
      7.2: f = 1 (33.33%)
        Frequency for column 'value':
          380: f = 1 (100.00%)
      7.4: f = 1 (33.33%)
        Frequency for column 'value':
          320: f = 1 (100.00%)
      9  : f = 1 (33.33%)
        Frequency for column 'value':
          420: f = 1 (100.00%)
"#,
                );
                return Ok((csv_builder, false));
            }

            let column_names = get_user_input_level_2("Enter column names separated by commas: ");

            if handle_cancel_flag(&column_names) {
                return Ok((csv_builder, false));
            }

            let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();
            println!();
            csv_builder.print_freq_cascading(columns);
        }

        "11" => {
            if action_flag == "d" {
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
                return Ok((csv_builder, false));
            }

            let column_name = get_user_input_level_2("Enter the column name: ");
            if handle_cancel_flag(&column_name) {
                return Ok((csv_builder, false));
            }

            csv_builder.print_unique(&column_name.trim());
        }

        "12" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Prints the number of unique values in a column, along with the mean and median of their frequencies.
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

  @LILbro: Enter column names separated by commas: value, interest

Statistics for column 'value':

  Total cumulative count of unique values: 10
  Mean frequency of the unique values: 1.00
  Median frequency of the unique values: 1.00

Statistics for column 'interest':

  Total cumulative count of unique values: 9
  Mean frequency of the unique values: 1.11
  Median frequency of the unique values: 1.00
"#,
                );
                return Ok((csv_builder, false));
            }

            let column_names = get_user_input_level_2("Enter column names separated by commas: ");

            if handle_cancel_flag(&column_names) {
                return Ok((csv_builder, false));
            }

            let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();
            csv_builder.print_unique_values_stats(columns);
        }

        "13" => {
            if action_flag == "d" {
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
                return Ok((csv_builder, false));
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
                    return Ok((csv_builder, false));
                }

                Err(e) => {
                    println!("Error getting filter expressions: {}", e);
                    return Ok((csv_builder, false));
                }
            }
        }
        "14" => {
            if action_flag == "d" {
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
                return Ok((csv_builder, false));
            }

            let column_names = get_user_input_level_2(
                "Enter the x-axis and y-axis column names (comma separated): ",
            );

            if handle_cancel_flag(&column_names) {
                return Ok((csv_builder, false));
            }

            let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();

            // Ensuring exactly two columns were provided.
            if columns.len() != 2 {
                // Handle the error: inform the user they need to enter exactly two column names.
                print_insight_level_2(
                    "Please enter exactly two column names, separated by a comma.",
                );
                return Ok((csv_builder, false));
            } else {
                // Extracting the column names.
                let x_axis_column = columns[0];
                let y_axis_column = columns[1];
                println!();
                // Using the columns in your function.
                csv_builder.print_dot_chart(x_axis_column, y_axis_column);
            }
        }
        "15" => {
            if action_flag == "d" {
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
                return Ok((csv_builder, false));
            }

            let column_names = get_user_input_level_2(
                "Enter the x-axis and y-axis column names (comma separated): ",
            );

            if handle_cancel_flag(&column_names) {
                return Ok((csv_builder, false));
            }

            let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();

            // Ensuring exactly two columns were provided.
            if columns.len() != 2 {
                // Handle the error: inform the user they need to enter exactly two column names.
                print_insight_level_2(
                    "Please enter exactly two column names, separated by a comma.",
                );
                return Ok((csv_builder, false));
            } else {
                // Extracting the column names.
                let x_axis_column = columns[0];
                let y_axis_column = columns[1];
                println!();
                // Using the columns in your function.
                csv_builder.print_cumulative_dot_chart(x_axis_column, y_axis_column);
            }
        }

        "16" => {
            if action_flag == "d" {
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
                return Ok((csv_builder, false));
            }

            let column_names = get_user_input_level_2(
                "Enter the x-axis and y-axis column names (comma separated): ",
            );

            if handle_cancel_flag(&column_names) {
                return Ok((csv_builder, false));
            }

            let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();

            // Ensuring exactly two columns were provided.
            if columns.len() != 2 {
                // Handle the error: inform the user they need to enter exactly two column names.
                print_insight_level_2(
                    "Please enter exactly two column names, separated by a comma.",
                );
                return Ok((csv_builder, false));
            } else {
                // Extracting the column names.
                let x_axis_column = columns[0];
                let y_axis_column = columns[1];
                println!();
                // Using the columns in your function.
                csv_builder.print_smooth_line_chart(x_axis_column, y_axis_column);
            }
        }

        "17" => {
            if action_flag == "d" {
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
                return Ok((csv_builder, false));
            }

            let column_names = get_user_input_level_2(
                "Enter the x-axis and y-axis column names (comma separated): ",
            );

            if handle_cancel_flag(&column_names) {
                return Ok((csv_builder, false));
            }

            let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();

            // Ensuring exactly two columns were provided.
            if columns.len() != 2 {
                // Handle the error: inform the user they need to enter exactly two column names.
                print_insight_level_2(
                    "Please enter exactly two column names, separated by a comma.",
                );
                return Ok((csv_builder, false));
            } else {
                // Extracting the column names.
                let x_axis_column = columns[0];
                let y_axis_column = columns[1];
                println!();
                // Using the columns in your function.
                csv_builder.print_cumulative_smooth_line_chart(x_axis_column, y_axis_column);
            }
        }

        "18" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

// Prints a cleanliness report if rows were to be cleaned by parsing columns with preset rules. Rows that do not conform to any of the stipulated rules are discarded.
  @LILbro: Executing this JSON query:
{
    "mobile": ["HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER", "HAS_LENGTH:10"],
    "price": [],
    "paid_on": ["IS_DATETIME_PARSEABLE"],
}

### AVAILABLE RULES

- "HAS_ONLY_NUMERICAL_VALUES"
- "HAS_ONLY_POSITIVE_NUMERICAL_VALUES"
- "HAS_LENGTH:10"
- "HAS_MIN_LENGTH:7"
- "HAS_MAX_LENGTH:12"
- "HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER"
- "HAS_NO_EMPTY_STRINGS"
- "IS_DATETIME_PARSEABLE"
"#,
                );
                return Ok((csv_builder, false));
            }

            if let Some(headers) = csv_builder.get_headers() {
                let mut json_array_str = "{\n".to_string();

                // Loop through headers and append them as keys in the JSON array string, excluding auto-computed columns
                for (i, header) in headers.iter().enumerate() {
                    if header != "id" && header != "c@" && header != "u@" {
                        json_array_str.push_str(&format!("    \"{}\": []", header));
                        if i < headers.len() - 1 {
                            json_array_str.push_str(",\n");
                        }
                    }
                }

                // Close the first JSON object and start the syntax explanation
                json_array_str.push_str("\n}");

                let syntax_explanation = r#"

SYNTAX
======

### Example

{
  "column1": ["HAS_ONLY_POSITIVE_NUMERICAL_VALUES", "HAS_NO_EMPTY_STRINGS"],
  "column2": [],
  "column3": ["HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER"],
  "column4": [],
  "column5": [],
  "column6": ["IS_DATETIME_PARSEABLE"],
  "column7": ["IS_DATETIME_PARSEABLE"]
}

### AVAILABLE RULES
- "HAS_ONLY_NUMERICAL_VALUES"
- "HAS_ONLY_POSITIVE_NUMERICAL_VALUES"
- "HAS_LENGTH:10"
- "HAS_MIN_LENGTH:7"
- "HAS_MAX_LENGTH:12"
- "HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER"
- "HAS_NO_EMPTY_STRINGS"
- "IS_DATETIME_PARSEABLE"

"#;

                let full_syntax = json_array_str + syntax_explanation;

                // Get user input
                let rows_json_str = get_edited_user_json_input(full_syntax);
                //dbg!(&rows_json_str);
                if handle_cancel_flag(&rows_json_str) {
                    return Ok((csv_builder, false));
                }

                // Parse the user input
                let rows_json: Value = match serde_json::from_str(&rows_json_str) {
                    Ok(json) => json,
                    Err(e) => {
                        eprintln!("Error parsing JSON string: {}", e);
                        return Err("An error occurred".to_string().into());
                    }
                };

                // Collect rules from user input
                let mut rules = Vec::new();
                if let Some(obj) = rows_json.as_object() {
                    for (key, value) in obj {
                        if let Some(rules_array) = value.as_array() {
                            let mut column_rules = Vec::new();
                            for rule in rules_array {
                                if let Some(rule_str) = rule.as_str() {
                                    if !rule_str.is_empty() {
                                        column_rules.push(rule_str.to_string());
                                    }
                                }
                            }
                            if !column_rules.is_empty() {
                                rules.push((key.clone(), column_rules));
                            }
                        }
                    }
                }

                //dbg!(&rules);
                println!();
                // Invoke the cleanliness report function with the collected rules
                csv_builder.print_cleanliness_report_by_column_parse(rules.clone());
                //  .clean_by_column_parse(rules.clone());

                if csv_builder.has_data() {
                    csv_builder.print_table();
                    println!();
                }
            }
        }

        _ => {
            println!("Invalid option. Please enter a number from 1 to 18.");
            return Ok((csv_builder, false));
        }
    }

    println!();
    return Ok((csv_builder, false));
}
