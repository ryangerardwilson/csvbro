// csv_appender.rs
use crate::csv_inspector::handle_inspect;
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

struct ExpStore {
    expressions: Vec<Exp<'static>>, // Store the Exp instances directly
}

#[derive(Debug, Clone)]
enum CompareValue<'a> {
    Single(&'a str),
    Multiple(Vec<&'a str>),
}

impl ExpStore {
    fn add_expression(
        &mut self,
        column: String,
        operator: String,
        compare_value: CompareValue,
        compare_type: String,
    ) {
        // Convert Strings into 'static references by leaking Box<str>
        let column_static = Box::leak(column.into_boxed_str());
        let operator_static = Box::leak(operator.into_boxed_str());
        let compare_type_static = Box::leak(compare_type.into_boxed_str());
        let exp;

        match compare_value {
            CompareValue::Single(value) => {
                //ExpVal::STR(Box::leak(value.to_string().into_boxed_str()))

                exp = Exp {
                    column: column_static,
                    operator: operator_static,
                    compare_with: ExpVal::STR(Box::leak(value.to_string().into_boxed_str())),
                    compare_as: compare_type_static,
                };
            }
            CompareValue::Multiple(values) => {
                // Step 1: Convert each `&str` to `String`
                let owned_values: Vec<String> = values.iter().map(|&val| val.to_string()).collect();

                // Step 2: Initialize an empty Vec<&'static str>
                let mut static_strs: Vec<&'static str> = Vec::new();

                // Step 3: Iterate over each `String` in `owned_values`, convert it to `Box<str>` and leak it
                for val in owned_values {
                    let leaked_str: &'static str = Box::leak(val.into_boxed_str());
                    static_strs.push(leaked_str);
                }

                //ExpVal::VEC(static_strs)

                exp = Exp {
                    column: column_static,
                    operator: operator_static,
                    compare_with: ExpVal::VEC(static_strs),
                    compare_as: compare_type_static,
                };
            }
        };

        self.expressions.push(exp);
    }

    fn get_exp(&self, index: usize) -> &Exp<'static> {
        &self.expressions[index]
    }
}

pub fn handle_append(csv_builder: &mut CsvBuilder) -> Result<(), Box<dyn std::error::Error>> {
    fn get_append_boolean_expression(
        data_store: &mut ExpStore,
    ) -> Result<(String, Vec<(String, usize)>, String), Box<dyn std::error::Error>> {
        let syntax = r#"{
  "new_column_name": "",
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
      "new_column_name": "is_new_small_purchase_customer",
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
        [
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
      "new_column_name": "lives_near_14_avenue_or_public_school",
      "expressions": [
        [
          "Exp1",
          {
            "column": "address"
            "operator": "FUZZ_MIN_SCORE_60",
            "compare_with": [
                "public school",
                "14 avenue",
              ],
            "compare_as": "TEXT"
          }
        ]
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

        let new_column_name = parsed_json["new_column_name"]
            .as_str()
            .ok_or("Invalid or missing new column name")?
            .to_string();

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

        Ok((new_column_name, expression_names, result_expression))
    }

    fn get_append_category_expression(
        data_store: &mut ExpStore,
    ) -> Result<(String, Vec<(String, Vec<(String, Exp)>, String)>), Box<dyn std::error::Error>>
    {
        let syntax = r#"{
  "new_column_name": "",
  "expressions": [
    {
      "category_name": "",
      "category_filters": [
        {
          "Exp1": {
            "column": "",
            "operator": "",
            "compare_with": "",
            "compare_as": ""
          }
        }
      ],
      "category_evaluation": "Exp1"
    },
    {
      "category_name": "",
      "category_filters": [
        {
          "Exp1": {
            "column": "",
            "operator": "",
            "compare_with": "",
            "compare_as": ""
          }
        },
        {
          "Exp2": {
            "column": "",
            "operator": "",
            "compare_with": "",
            "compare_as": ""
          }
        }
      ],
      "category_evaluation": "Exp1 && Exp2"
    }
  ]
}


SYNTAX
======

1. NUMBERS/TIMESTAMPS/TEXT Operations
------------------------------------

### Expression

    {
      "new_column_name": "EXPENSE_RANGE",
      "expressions":  [
            [
                category_name: "< 1000",
                category_filters: [
                  [
                    "Exp1", 
                    {
                      column: "Withdrawal Amt.",
                      operator: "<",
                      compare_with: ExpVal::STR("1000"),
                      compare_as: "NUMBERS" // Also: "TEXT", "TIMESTAMPS"
                    }
                  ]
                ],
                category_evaluation: "Exp1"
            ],
            [
                category_name: "1000-5000",
                category_filters: [ 
                  [
                    "Exp1", 
                    {
                      column: "Withdrawal Amt.",
                      operator: ">=",
                      compare_with: ExpVal::STR("1000"),
                      compare_as: "NUMBERS"
                    }
                  ],
                    "Exp2", 
                    {
                       column: "Withdrawal Amt.",
                       operator: "<",
                       compare_with: ExpVal::STR("5000"),
                       compare_as: "NUMBERS"
                    }
                  ]
                "Exp1 && Exp2"
            ]
        ]
    }


### Available Operators

- NUMBERS/TIMESTAMPS (==, !=, >, <, >=, <=)
- TEXT (==, !=, CONTAINS, STARTS_WITH, DOES_NOT_CONTAIN)

2. VECTOR/ARRAY Operations
--------------------------

### Expression

{
  "new_column_name": "EXPENSE_RANGE",
  "expressions":  [
        [
            category_name: "lives_near_14_avenue_or_public_school",
            category_filters: [
              [
                "Exp1", 
                {
                  column: "address",
                  operator: "FUZZ_MIN_SCORE_60",
                  compare_with: [
                      "public school",
                      "14 avenue"
                    ],
                  compare_as: "TEXT"
                }
              ]
            ],
            category_evaluation: "Exp1"
        ],
    ]
}

### Available Operators

- FUZZ_MIN_SCORE_10/20/30, etc.

  "#;

        let exp_json = get_edited_user_json_input((&syntax).to_string());
        let parsed_json: Value = serde_json::from_str(&exp_json)?;

        let new_column_name = parsed_json["new_column_name"]
            .as_str()
            .ok_or("Invalid or missing new column name")?
            .to_string();

        let categories_array = parsed_json["expressions"]
            .as_array()
            .ok_or("Invalid format for expressions")?;

        let mut categories = Vec::new();

        for (category_index, category) in categories_array.iter().enumerate() {
            let category_name = category["category_name"]
                .as_str()
                .ok_or(format!(
                    "Invalid or missing category name for category {}",
                    category_index
                ))?
                .to_string();

            let filters_array = category["category_filters"].as_array().ok_or(format!(
                "Invalid format for category filters in category {}",
                category_index
            ))?;

            let mut filters = Vec::new();

            for filter in filters_array.iter() {
                for (exp_name, expression_details) in filter.as_object().ok_or(format!(
                    "Invalid format for filter in category {}",
                    category_index
                ))? {
                    let column = expression_details["column"]
                        .as_str()
                        .ok_or(format!(
                            "Invalid or missing column for expression '{}' in category {}",
                            exp_name, category_index
                        ))?
                        .to_string();

                    let operator = expression_details["operator"]
                        .as_str()
                        .ok_or(format!(
                            "Invalid or missing operator for expression '{}' in category {}",
                            exp_name, category_index
                        ))?
                        .to_string();

                    let compare_value = if let Some(compare_with_array) =
                        expression_details["compare_with"].as_array()
                    {
                        CompareValue::Multiple(
                            compare_with_array
                                .iter()
                                .filter_map(|v| v.as_str())
                                //.map(|s| s.to_string())
                                .collect(),
                        )
                    } else if let Some(compare_with_single) =
                        expression_details["compare_with"].as_str()
                    {
                        CompareValue::Single(compare_with_single)
                    } else {
                        //return Err(Box::new(std::fmt::Error::new(format!("Invalid or missing compare_with for filter {} in category {}", exp_name, category_index))));
                        return Err(Box::from(format!(
                            "Invalid or missing compare_with for filter {} in category {}",
                            exp_name, category_index
                        )));
                    };

                    let compare_type = expression_details["compare_as"]
                        .as_str()
                        .ok_or(format!(
                            "Invalid or missing compare_as for expression '{}' in category {}",
                            exp_name, category_index
                        ))?
                        .to_string();

                    data_store.add_expression(column, operator, compare_value, compare_type);

                    let exp = data_store.get_exp(data_store.expressions.len() - 1).clone();
                    filters.push((exp_name.to_string(), exp));
                }
            }

            let category_evaluation = category["category_evaluation"]
                .as_str()
                .ok_or(format!(
                    "Invalid or missing category evaluation for category {}",
                    category_index
                ))?
                .to_string();

            categories.push((category_name, filters, category_evaluation));
        }

        Ok((new_column_name, categories))
    }

    let menu_options = vec![
        "Append derived boolean column",
        "Append derived category column",
        "Inspect",
        "Show all rows",
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
                let mut exp_store = ExpStore {
                    expressions: Vec::new(),
                };

                match get_append_boolean_expression(&mut exp_store) {
                    Ok((new_column_name, expression_names, result_expression)) => {
                        // Check if the new column name is empty
                        if new_column_name.trim().is_empty() {
                            print_insight_level_2(
                                "No new column name provided. Operation aborted.",
                            );
                            continue; // Skip the rest of the process
                        }

                        let expressions_refs: Vec<(&str, Exp)> = expression_names
                            .iter()
                            .map(|(name, index)| (name.as_str(), exp_store.get_exp(*index).clone()))
                            .collect();

                        // Append the new derived column
                        csv_builder.append_derived_boolean_column(
                            &new_column_name,
                            expressions_refs,
                            &result_expression,
                        );
                        if csv_builder.has_data() {
                            csv_builder.print_table();
                            println!();
                        }
                        print_insight_level_2("Derived boolean column appended.");
                    }
                    Err(e) => {
                        println!("Error getting expressions: {}", e);
                        continue; // Return to the menu to let the user try again or choose another option
                    }
                }
            }

            Some(2) => {
                let mut exp_store = ExpStore {
                    expressions: Vec::new(),
                };

                match get_append_category_expression(&mut exp_store) {
                    Ok((new_column_name, category_expressions)) => {
                        if new_column_name.trim().is_empty() {
                            print_insight_level_2(
                                "No new column name provided. Operation aborted.",
                            );
                            continue;
                        }

                        let mut string_storage = Vec::new();
                        // First pass: collect all strings
                        for (cat_name, filters, cat_eval) in &category_expressions {
                            string_storage.push(cat_name.clone());
                            for (filter_name, _) in filters {
                                string_storage.push(filter_name.clone());
                            }
                            string_storage.push(cat_eval.clone());
                        }

                        // Second pass: create references
                        let mut string_index = 0;
                        let category_expressions: Vec<(&str, Vec<(&str, Exp)>, &str)> =
                            category_expressions
                                .into_iter()
                                .map(|(_, filters, _)| {
                                    let cat_name = &string_storage[string_index];
                                    string_index += 1;

                                    let filters: Vec<(&str, Exp)> = filters
                                        .into_iter()
                                        .map(|(_, exp)| {
                                            let filter_name = &string_storage[string_index];
                                            string_index += 1;
                                            (filter_name.as_str(), exp)
                                        })
                                        .collect();

                                    let cat_eval = &string_storage[string_index];
                                    string_index += 1;

                                    (cat_name.as_str(), filters, cat_eval.as_str())
                                })
                                .collect();

                        csv_builder
                            .append_derived_category_column(&new_column_name, category_expressions);
                        if csv_builder.has_data() {
                            csv_builder.print_table();
                            println!();
                        }
                        print_insight_level_2("Derived category column appended.");
                    }
                    Err(e) => {
                        println!("Error getting expressions: {}", e);
                        continue;
                    }
                }
            }

            Some(3) => {
                if let Err(e) = handle_inspect(csv_builder) {
                    println!("Error during inspection: {}", e);
                    continue;
                }
            }

            Some(4) => {
                if csv_builder.has_data() {
                    csv_builder.print_table_all_rows();
                    println!();
                }
            }
            Some(5) => {
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
