// csv_inspector.rs
use crate::user_interaction::{
    //get_edited_user_sql_input,
    get_edited_user_json_input,
    get_user_input_level_2,
    print_insight_level_2,
    print_list,
};

use fuzzywuzzy::fuzz;
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

pub fn handle_inspect(csv_builder: &mut CsvBuilder) -> Result<(), Box<dyn std::error::Error>> {
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

1. NUMBERS/TIMESTAMPS/TEXT Operations
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
          },
        ],
        [
          "Exp2",
          {
            "column": "added_at"
            "operator": ">",
            "compare_with": "2024-01-01 00:00:00",
            "compare_as": "TIMESTAMPS"
          },
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

2. VECTOR/ARRAY Operations
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

            /*
            let compare_value = if let Some(compare_with_array) =
                exp.get(1).and_then(|cw| cw["compare_with"].as_array())
            {
                CompareValue::Multiple(
                    compare_with_array
                        .iter()
                        .filter_map(|v| v.as_str())
                        .collect(),
                ) // Collecting as Vec<&str>
            } else if let Some(compare_with_single) =
                exp.get(1).and_then(|cw| cw["compare_with"].as_str())
            {
                CompareValue::Single(compare_with_single)
            } else {
                return Err("Invalid or missing compare_with".into());
            };
            */
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
        "Print first row",
        "Print last row",
        "Print rows",
        "Print rows where",
        "Print freq of multiple column values",
        "Print unique column values",
        "Print count where",
        "Go back",
    ];

    loop {
        print_insight_level_2("Select an option to inspect CSV data:");

        for (index, option) in menu_options.iter().enumerate() {
            print_list(&format!("{}: {}", index + 1, option));
        }

        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        let mut selected_option = None;

        // Check for direct numeric input
        if let Ok(index) = choice.parse::<usize>() {
            if index > 0 && index <= menu_options.len() {
                selected_option = Some(index);
            }
        }

        // If no direct numeric input, use fuzzy matching
        if selected_option.is_none() {
            let (best_match_index, _) = menu_options
                .iter()
                .enumerate()
                .map(|(index, option)| (index + 1, fuzz::ratio(&choice, &option.to_lowercase())))
                .max_by_key(|&(_, score)| score)
                .unwrap_or((0, 0));

            if best_match_index > 0 && best_match_index <= menu_options.len() {
                selected_option = Some(best_match_index);
            }
        }

        match selected_option {
            Some(1) => {
                csv_builder.print_first_row();
            }
            Some(2) => {
                csv_builder.print_last_row();
            }
            Some(3) => {
                let start = get_user_input_level_2("Enter the start row number: ")
                    .parse::<usize>()
                    .map_err(|_| "Invalid start row number")?;

                let end = get_user_input_level_2("Enter the end row number: ")
                    .parse::<usize>()
                    .map_err(|_| "Invalid end row number")?;

                csv_builder.print_rows_range(start, end);
            }
            Some(4) => {
                let mut exp_store = ExpStore {
                    expressions: Vec::new(),
                };

                match get_filter_expressions(&mut exp_store) {
                    Ok((expression_names, result_expression)) => {
                        let expressions_refs: Vec<(&str, Exp)> = expression_names
                            .iter()
                            .map(|(name, index)| (name.as_str(), exp_store.get_exp(*index).clone()))
                            .collect();

                        //dbg!(&expressions_refs, &result_expression);
                        csv_builder.print_rows_where(expressions_refs, &result_expression);
                    }
                    Err(e) => {
                        println!("Error getting filter expressions: {}", e);
                        continue; // Return to the menu to let the user try again or choose another option
                    }
                }
            }
            Some(5) => {
                let column_names =
                    get_user_input_level_2("Enter column names separated by commas: ");
                let columns: Vec<&str> = column_names.split(',').map(|s| s.trim()).collect();
                csv_builder.print_freq(columns);
            }
            Some(6) => {
                let column_name = get_user_input_level_2("Enter the column name: ");
                csv_builder.print_unique(&column_name.trim());
            }

            // In your handle_inspect method
            Some(7) => {
                let mut exp_store = ExpStore {
                    expressions: Vec::new(),
                };

                match get_filter_expressions(&mut exp_store) {
                    Ok((expression_names, result_expression)) => {
                        let expressions_refs: Vec<(&str, Exp)> = expression_names
                            .iter()
                            .map(|(name, index)| (name.as_str(), exp_store.get_exp(*index).clone()))
                            .collect();

                        csv_builder.print_count_where(expressions_refs, &result_expression);
                    }
                    Err(e) => {
                        println!("Error getting filter expressions: {}", e);
                        continue; // Return to the menu to let the user try again or choose another option
                    }
                }
            }

            Some(8) => {
                break; // Exit the inspect handler
            }
            _ => {
                println!("Invalid option. Please enter a number from 1 to 6.");
                continue; // Ask for the choice again
            }
        }

        println!(); // Print a new line for better readability
    }

    Ok(())
}
