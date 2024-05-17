// csv_pivoter.rs
use crate::config::Config;
use crate::user_experience::{
    handle_back_flag, handle_cancel_flag, handle_quit_flag, handle_special_flag,
};
use crate::user_interaction::{
    determine_action_as_number, get_edited_user_json_input, get_user_input_level_2,
    print_insight_level_2, print_list_level_2,
};
use rgwml_heavy::csv_utils::{CsvBuilder, Exp, ExpVal, Piv, Train};
use serde_json::from_str;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::fs::read_to_string;
use std::path::Path;
use std::path::PathBuf;

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

pub async fn handle_pivot(
    csv_builder: &mut CsvBuilder,
    file_path_option: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    fn apply_filter_changes_menu(
        csv_builder: &mut CsvBuilder,
        prev_iteration_builder: &CsvBuilder,
        original_csv_builder: &CsvBuilder,
    ) -> Result<(), String> {
        let menu_options = vec![
            "Continue with modified data",
            "Discard this result, and load previous result",
            "Load original, to modify from scratch",
        ];
        print_insight_level_2("Apply changes?");
        print_list_level_2(&menu_options);

        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        let selected_option = determine_action_as_number(&menu_options, &choice);

        match selected_option {
            Some(1) => {
                print_insight_level_2("Continuing with modified data");
                csv_builder.print_table();
                // Implement the logic for continuing with filtered data
                Ok(())
            }
            Some(2) => {
                print_insight_level_2("Discarding this result, and loading previous result");
                csv_builder
                    .override_with(prev_iteration_builder)
                    .print_table();
                Ok(())
            }
            Some(3) => {
                print_insight_level_2("Loading original data, for you to modify from scratch");
                csv_builder
                    .override_with(original_csv_builder)
                    .print_table();
                Ok(())
            }
            _ => Err("Invalid option. Please enter a number from 1 to 3.".to_string()),
        }
    }

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
  "evaluation": "Exp1"
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

        if handle_cancel_flag(&exp_json) {
            return Err("Operation canceled".into());
        }

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
                        .map(|s| s.to_string()) // Convert &str to String
                        .collect(), // Collecting as Vec<String>
                )
            } else if let Some(compare_with_single) =
                exp.get(1).and_then(|cw| cw["compare_with"].as_str())
            {
                CompareValue::Single(compare_with_single.to_string()) // Convert &str to String
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

    fn get_append_open_ai_analysis_expression(
    ) -> Result<(Vec<String>, HashMap<String, String>, String), Box<dyn Error>> {
        let syntax = r#"{
  "target_columns": [],
  "analysis_query": {
    "": "",
    "": ""
  },
  "model": "gpt-3.5-turbo-0125"
}

SYNTAX
======

{
  "target_columns": ["transcribed_text", "count_of_complaints"],
  "analysis_query": {
    "customer_query": "extract the gist of the query raised by customer in the conversation text",
    "agent_response": "extract the gist of the response given by agent to customer in the conversation text"
  },
  "model": "gpt-3.5-turbo-0125" // Other compatible models inlcude "gpt-4-turbo-preview"
}

  "#;

        let exp_json = get_edited_user_json_input((&syntax).to_string());

        if handle_cancel_flag(&exp_json) {
            return Err("Operation canceled".into());
        }

        //dbg!(&exp_json);

        let parsed_json: Value = serde_json::from_str(&exp_json)?;

        //dbg!(&parsed_json);

        // Extract target columns
        let target_columns = parsed_json["target_columns"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<Vec<String>>();

        let model = parsed_json["model"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        // Extract analysis_query and convert it to HashMap<String, String>
        let analysis_query_json = &parsed_json["analysis_query"];
        let mut analysis_query = HashMap::new();
        if let Some(obj) = analysis_query_json.as_object() {
            for (key, value) in obj {
                if let Some(val_str) = value.as_str() {
                    analysis_query.insert(key.clone(), val_str.to_string());
                }
            }
        }

        Ok((target_columns, analysis_query, model))
    }

    fn get_append_linear_regression_expression() -> Result<
        (String, Vec<Vec<String>>, Vec<f64>, Vec<f64>, Vec<String>),
        Box<dyn std::error::Error>,
    > {
        let syntax = r#"{
  "new_column_name": "",
  "output_range": [0, 100],
  "training_data": [
   [
     "",
     {
       "": "",
       "": ""
     }
   ],
   [
     "",
     {
       "": "",
       "": ""
     }
   ]
  ]
}

SYNTAX
======

{
  "new_column_name": "predictions",
  "output_range": [20.2, 100],
  "training_data": [
   [
     "90",
     {
       "action": "told_outage",
       "agent_name": "ANIKET"
     }
   ],
   [
     "70",
     {
       "action": "told_plan_inactive",
       "agent_name": "ANIKET"
     }
   ],
   [
     "60",
     {
       "action": "ticketing",
       "agent_name": "Vishal"
     }
   ],
   [
     "50",
     {
       "action": "ticketing",
       "agent_name": "Ankita"
     }
   ]
  ]
}

  "#;

        let exp_json = get_edited_user_json_input((&syntax).to_string());

        if handle_cancel_flag(&exp_json) {
            return Err("Operation canceled".into());
        }

        //dbg!(&exp_json);

        // Assume `last_exp_json` is a String containing your JSON data
        let parsed_json: Value = serde_json::from_str(&exp_json)?;

        //dbg!(&parsed_json);

        // Assuming `parsed_json` is already defined and contains the user input data

        // Extract new column name
        let new_column_name = parsed_json["new_column_name"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        // Initialize vectors to hold training predictors and outputs
        let mut training_predictors = Vec::new();
        let mut training_outputs = Vec::new();

        // Deduce column names from the first item of the training data
        let first_item = parsed_json["training_data"][0][1].as_object().unwrap();
        let predictor_column_names: Vec<String> = first_item.keys().cloned().collect();

        // Parse training data
        for item in parsed_json["training_data"].as_array().unwrap() {
            let outcome = item[0]
                .as_str()
                .unwrap_or_default()
                .parse::<f64>()
                .unwrap_or_default();
            let data_object = item[1].as_object().unwrap();

            let mut row = Vec::new();
            for key in &predictor_column_names {
                let value = data_object[key].as_str().unwrap_or_default();
                row.push(value.to_string());
            }

            training_predictors.push(row);
            training_outputs.push(outcome);
        }

        // Parse output range
        let output_range = parsed_json["output_range"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap_or_default())
            .collect::<Vec<f64>>();

        // Return the structured data
        Ok((
            new_column_name,
            training_predictors,
            training_outputs,
            output_range,
            predictor_column_names,
        ))

        /*
            // You will need to implement the logic for gathering and parsing this data from user input.
            Ok(("Predictions".to_string(), vec![vec!["90".to_string(), "95".to_string()], vec!["70".to_string(), "72".to_string()]], vec![72.0, 65.0], vec![0.0, 100.0], vec!["Feature1".to_string(), "Feature2".to_string()]))
        */
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

        if handle_cancel_flag(&exp_json) {
            return Err("Operation canceled".into());
        }

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
                                .map(|s| s.to_string()) // Convert &str to String
                                .collect(), // Collecting as Vec<String>
                        )
                    } else if let Some(compare_with_single) =
                        expression_details["compare_with"].as_str()
                    {
                        CompareValue::Single(compare_with_single.to_string()) // Convert &str to String
                    } else {
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

    fn get_concatenation_input() -> Result<(String, Vec<String>), Box<dyn Error>> {
        // Placeholder for getting JSON input from the user
        let json_syntax = r#"{
    "new_column_name": "",
    "concatenation_items": []
}

SYNTAX
======

{
    "new_column_name": "ConcatenatedResultColumn",
    "concatenation_items": ["Column1", " ", "Column2"]
}

"#;

        // Simulating user editing JSON input and providing it back
        let user_edited_json = get_edited_user_json_input(json_syntax.to_string());

        if handle_cancel_flag(&user_edited_json) {
            return Err("Operation canceled".into());
        }

        let parsed_json: Value = serde_json::from_str(&user_edited_json)?;

        let new_column_name = parsed_json["new_column_name"]
            .as_str()
            .ok_or("Invalid or missing new column name")?
            .to_string();

        let concatenation_items = parsed_json["concatenation_items"]
            .as_array()
            .ok_or("Invalid format for concatenation items")?
            .iter()
            .filter_map(|item| item.as_str().map(String::from))
            .collect();

        Ok((new_column_name, concatenation_items))
    }

    fn get_date_split_input() -> Result<(String, String), Box<dyn Error>> {
        let syntax = r#"{
    "column_name": "",
    "date_format": ""
}

SYNTAX
======

{
    "column_name": "created_at",
    "data_format": "%Y-%m-%d %H:%M:%S%.f"
}

- %Y-%m-%d: 2023-01-30.
- %Y-%m-%d %H:%M:%S: 2023-01-30 15:45:30.
- %Y/%m/%d: 2023/01/30
- %d-%m-%Y: 30-01-2023.
- %Y-%m-%d %H:%M:%S%.f: 2024-02-03 10:42:07.856666666
- %b %d, %Y: Jan 30, 2023.

"#;

        let date_split_json = get_edited_user_json_input(syntax.to_string());

        if handle_cancel_flag(&date_split_json) {
            return Err("Operation canceled".into());
        }

        let parsed_json: Value = serde_json::from_str(&date_split_json)?;

        let column_name = parsed_json["column_name"]
            .as_str()
            .ok_or("Invalid or missing column name")?
            .to_string();

        let date_format = parsed_json["date_format"]
            .as_str()
            .ok_or("Invalid or missing date format")?
            .to_string();

        Ok((column_name, date_format))
    }

    fn get_fuzzai_analysis_input(
    ) -> Result<(String, String, Vec<Train>, String, String, String), Box<dyn Error>> {
        let syntax = r#"{
    "column_to_analyze": "",
    "column_prefix": "",
    "training_data": [
        {"input": "", "output": ""},
        {"input": "", "output": ""}
    ],
    "word_split_param": "WORD_SPLIT:2",
    "word_length_sensitivity_param": "WORD_LENGTH_SENSITIVITY:0.8",
    "get_best_param": "GET_BEST:2"
}

SYNTAX
======

{
    "column_to_analyze": "Column1",
    "column_prefix": "sales_analysis",
    "training_data": [
            {"input": "I want my money back", "output": "refund"},
            {"input": "I want a refund immediately", "output": "refund"}
        ],
    "word_split_param": "WORD_SPLIT:2",
    "word_length_sensitivity_param": "WORD_LENGTH_SENSITIVITY:0.8",
    "get_best_param": "GET_BEST:2",
}

Note the implications of the params in the JSON query:
1. "column to analyze": The name of the column to be subject to fuzzy analysis against your training data.
2. "column_prefix": The prefix of the newly created columns.
3. "training_data": Your training data. It is good practice to anticipate as many outcomes as possible.
4. "word_split_param": The word length of the smallest combination the value and the training data that would be split to ascertain the best score.
5. "word_length_sensitivity_param": Whether the fuzzy analysis score should be adjusted to give closer matches in the event the word length of the training inputs are more similar to the word length of the column value. Values can range from 0.0 to 1.0, with values closer to 1.0 resulting in higher scores where the rival words lengths are similar.
6. "get_best_param": Determines the number of fuzzy analysis results that should be provided. A value of 1 would get the best match, where as a value of 2 would also return the second best match. This can have a maximum value of 3.
    "#;

        // Assume get_edited_user_json_input allows user to edit the predefined syntax
        let fuzzai_json = get_edited_user_json_input(syntax.to_string());

        if handle_cancel_flag(&fuzzai_json) {
            return Err("Operation canceled".into());
        }

        let parsed_json: Value = serde_json::from_str(&fuzzai_json)?;

        // Extract and construct each parameter
        let column_to_analyze = parsed_json["column_to_analyze"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let column_prefix = parsed_json["column_prefix"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let training_data: Vec<Train> = parsed_json["training_data"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .map(|train| Train {
                input: train["input"].as_str().unwrap_or("").to_string(),
                output: train["output"].as_str().unwrap_or("").to_string(),
            })
            .collect();

        let word_split_param = parsed_json["word_split_param"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let word_length_sensitivity_param = parsed_json["word_length_sensitivity_param"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let get_best_param = parsed_json["get_best_param"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        Ok((
            column_to_analyze,
            column_prefix,
            training_data,
            word_split_param,
            word_length_sensitivity_param,
            get_best_param,
        ))
    }

    fn get_fuzzai_analysis_where_input() -> Result<
        (
            String,
            String,
            Vec<Train>,
            String,
            String,
            String,
            Vec<(String, Exp)>,
            String,
        ),
        Box<dyn Error>,
    > {
        let syntax = r#"{
    "column_to_analyze": "",
    "column_prefix": "",
    "training_data": [
        {"input": "", "output": ""},
        {"input": "", "output": ""}
    ],
    "word_split_param": "WORD_SPLIT:2",
    "word_length_sensitivity_param": "WORD_LENGTH_SENSITIVITY:0.8",
    "get_best_param": "GET_BEST:2",
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
    "result_expression": "Exp1"
}

SYNTAX
======

{
    "column_to_analyze": "Column1",
    "column_prefix": "sales_analysis",
    "training_data": [
            {"input": "I want my money back", "output": "refund"},
            {"input": "I want a refund immediately", "output": "refund"}
        ],
    "word_split_param": "WORD_SPLIT:2",
    "word_length_sensitivity_param": "WORD_LENGTH_SENSITIVITY:0.8",
    "get_best_param": "GET_BEST:2",
    "expressions": [
            [
                "Exp1",
                {
                    "column": "Deposit Amt.",
                    "operator": ">",
                    "compare_with": "500",
                    "compare_as": "NUMBERS"
                }
            ]
        ]
    "result_expression": "Exp1"
}

Note the implications of the params in the JSON query:
1. "column to analyze": The name of the column to be subject to fuzzy analysis against your training data.
2. "column_prefix": The prefix of the newly created columns.
3. "training_data": Your training data. It is good practice to anticipate as many outcomes as possible.
4. "word_split_param": The word length of the smallest combination the value and the training data that would be split to ascertain the best score.
5. "word_length_sensitivity_param": Whether the fuzzy analysis score should be adjusted to give closer matches in the event the word length of the training inputs are more similar to the word length of the column value. Values can range from 0.0 to 1.0, with values closer to 1.0 resulting in higher scores where the rival words lengths are similar.
6. "get_best_param": Determines the number of fuzzy analysis results that should be provided. A value of 1 would get the best match, where as a value of 2 would also return the second best match. This can have a maximum value of 3.
7. "expressions" and "result_expression": Indicates the exact conditions of the row, that should trigger the fuzzy analysis.
    "#;

        // Assume get_edited_user_json_input allows user to edit the predefined syntax
        let fuzzai_json = get_edited_user_json_input(syntax.to_string());

        if handle_cancel_flag(&fuzzai_json) {
            return Err("Operation canceled".into());
        }

        let parsed_json: Value = serde_json::from_str(&fuzzai_json)?;

        // Extract and construct each parameter
        let column_to_analyze = parsed_json["column_to_analyze"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let column_prefix = parsed_json["column_prefix"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let training_data: Vec<Train> = parsed_json["training_data"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .map(|train| Train {
                input: train["input"].as_str().unwrap_or("").to_string(),
                output: train["output"].as_str().unwrap_or("").to_string(),
            })
            .collect();

        let word_split_param = parsed_json["word_split_param"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let word_length_sensitivity_param = parsed_json["word_length_sensitivity_param"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let get_best_param = parsed_json["get_best_param"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let expressions: Vec<(String, Exp)> = parsed_json["expressions"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .map(|exp| {
                (
                    exp[0].as_str().unwrap_or_default().to_string(),
                    Exp {
                        column: exp[1]["column"].as_str().unwrap_or("").to_string(),
                        operator: exp[1]["operator"].as_str().unwrap_or("").to_string(),
                        compare_with: match exp[1]["compare_with"].as_str() {
                            Some(value) => ExpVal::STR(value.to_string()),
                            None => ExpVal::STR("".to_string()), // Adjust as necessary for your logic
                        },
                        compare_as: exp[1]["compare_as"].as_str().unwrap_or("").to_string(),
                    },
                )
            })
            .collect();

        let result_expression = parsed_json["result_expression"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        Ok((
            column_to_analyze,
            column_prefix,
            training_data,
            word_split_param,
            word_length_sensitivity_param,
            get_best_param,
            expressions,
            result_expression,
        ))
    }

    fn get_pivot_input() -> Result<(Piv, String), Box<dyn Error>> {
        let pivot_syntax = r#"{
    "index_at": "",
    "values_from": "",
    "operation": "",
    "seggregate_by": [
        {"column": "", "type": ""}
    ],
    "save_as": ""
}

SYNTAX
======

{
    "index_at": "Date",
    "values_from": "Sales",
    "operation": "MEDIAN", // Also "COUNT", "COUNT_UNIQUE", "SUM", "MEAN", "BOOL_PERCENT" (assuming column values of 0 or 1 in 'values_from', calculates the % of 1 values for the segment)
    "seggregate_by": [  // Leave as empty [] if seggregation is not required
        {"column": "Category", "type": "AS_CATEGORY"},
        {"column": "IsPromotion", "type": "AS_BOOLEAN"}
    ],
    "save_as": "analysis1" // Leave as "" if you dont want to save it
}

Note the implication of params in the Json Query:
1. "index_at": This parameter determines the primary key column of the pivot table, or the field by which the data will be grouped vertically (row labels). It's the main dimension of analysis. This can be either a text or a number, depending on the data you are grouping by. For example, if you are grouping sales data by region, index_at could be the name of the region (text). If you are grouping by year, it could be the year (number).
2. "values_from": Specifies the column(s) from which to retrieve the values that will be summarized or aggregated in the pivot table. This would be a column with numerical data since you are usually performing operations like sums, averages, counts, etc.
3. "operation": Defines the type of aggregation or summarization to perform on the values_from data across the grouped index_at categories. Operations include "COUNT", "COUNT_UNIQUE", "SUM", "MEAN", "BOOL_PERCENT" (assuming column values of 0 or 1 in 'values_from', calculates the % of 1 values for the segment)
4. "seggregate_by": This parameter allows for additional segmentation of data within the primary grouping defined by index_at. Each segment within seggregate_by can further divide the data based on the specified column and the type of segmentation (like categorical grouping or binning numerical data into ranges).
- 4.1. Column: Can be both text or number, similar to index_at, depending on what additional dimension you want to segment the data by.
- 4.2. Type: Is text, indicating how the segmentation should be applied. The column specified can have a type of "AS_CATEGORY", or "AS_BOOLEAN"
  - 4.2.1. AS_CATEGORY: It means that each unique value in the specified seggregation column will create a separate subgroup within each primary group. This is appropriate for text data or numerical data that represent distinct categories or groups rather than values to be aggregated.
  - 4.2.2. AS_BOOLEAN: By setting the type to "AS_BOOLEAN", it's understood that the specified seggregation column contains boolean values (1/0). The data will be segmented into two groups based on these boolean values. This type is particularly useful for flag columns that indicate the presence or absence of a particular condition or attribute.
"#;

        let user_input = get_edited_user_json_input(pivot_syntax.to_string());

        if handle_cancel_flag(&user_input) {
            return Err("Operation canceled".into());
        }

        let parsed_json: Value = serde_json::from_str(&user_input)?;

        let index_at = parsed_json["index_at"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let values_from = parsed_json["values_from"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let operation = parsed_json["operation"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let seggregate_by: Vec<(String, String)> = parsed_json["seggregate_by"]
            .as_array()
            .map_or_else(Vec::new, |items| {
                items
                    .iter()
                    .filter_map(|item| {
                        let column = item["column"].as_str().unwrap_or("").to_string();
                        let type_ = item["type"].as_str().unwrap_or("").to_string();
                        if column.is_empty() || type_.is_empty() {
                            None // Exclude items where either column or type is empty
                        } else {
                            Some((column, type_)) // Include valid segregation criteria
                        }
                    })
                    .collect()
            });

        let save_as_path = parsed_json["save_as"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        Ok((
            Piv {
                index_at,
                values_from,
                operation,
                seggregate_by,
            },
            save_as_path,
        ))
    }

    let menu_options = vec![
        "APPEND DERIVED BOOLEAN COLUMN",
        "APPEND DERIVED CATEGORY COLUMN",
        "APPEND DERIVED CONCATENATION COLUMN",
        "APPEND CATEGORY COLUMNS BY SPLITTING DATE/TIMESTAMP COLUMN",
        "APPEND FUZZAI ANALYSIS COLUMN",
        "APPEND FUZZAI ANALYSIS COLUMN WHERE",
        "APPEND OPENAI ANALYSIS COLUMNS",
        "APPEND LINEAR REGRESSION COLUMN",
        "PIVOT",
    ];

    let original_csv_builder = CsvBuilder::from_copy(csv_builder);

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
        let prev_iteration_builder = CsvBuilder::from_copy(csv_builder);

        match selected_option {
            Some(1) => {
                if choice.to_lowercase() == "1d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Appends a column whose value would be either 0 or 1, contingent on the evaluation of conditions.

{
  "new_column_name": "is_big_expense",
  "expressions": [
    [
      "Exp1",
      {
        "column": "value",
        "operator": ">",
        "compare_with": "1000",
        "compare_as": "NUMBERS"
      }
    ]
  ],
  "evaluation": "Exp1"
}

|id |item    |value |is_big_expense |
-------------------------------------
|1  |books   |1000  |0              |
|2  |snacks  |200   |0              |
|3  |cab fare|300   |0              |
|4  |rent    |20000 |1              |
|5  |movies  |1500  |1              |
Total rows: 5
"#,
                    );
                    continue;
                }
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

                        match apply_filter_changes_menu(
                            csv_builder,
                            &prev_iteration_builder,
                            &original_csv_builder,
                        ) {
                            Ok(_) => (),
                            Err(e) => {
                                println!("{}", e);
                                continue; // Ask for the choice again if there was an error
                            }
                        }
                    }
                    Err(e) if e.to_string() == "Operation canceled" => {
                        continue;
                    }
                    Err(e) => {
                        println!("Error getting expressions: {}", e);
                        continue; // Return to the menu to let the user try again or choose another option
                    }
                }
            }

            Some(2) => {
                if choice.to_lowercase() == "2d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Appends a column whose value would be assigned category flags, contingent on the evaluation of conditions.

{
  "new_column_name": "type",
  "expressions": [
    {
      "category_name": "big",
      "category_filters": [
        {
          "Exp1": {
            "column": "value",
            "operator": ">",
            "compare_with": "5000",
            "compare_as": "NUMBERS"
          }
        }
      ],
      "category_evaluation": "Exp1"
    },
    {
      "category_name": "medium",
      "category_filters": [
        {
          "Exp1": {
            "column": "value",
            "operator": ">",
            "compare_with": "1000",
            "compare_as": "NUMBERS"
          }
        },
        {
          "Exp2": {
            "column": "value",
            "operator": "<",
            "compare_with": "5000",
            "compare_as": "NUMBERS"
          }
        }
      ],
      "category_evaluation": "Exp1 && Exp2"
    },
    {
      "category_name": "small",
      "category_filters": [
        {
          "Exp1": {
            "column": "value",
            "operator": "<",
            "compare_with": "1000",
            "compare_as": "NUMBERS"
          }
        }
      ],
      "category_evaluation": "Exp1"
    }
  ]
}

|id |item    |value |type         |
-----------------------------------
|1  |books   |1000  |Uncategorized|
|2  |snacks  |200   |small        |
|3  |cab fare|300   |small        |
|4  |rent    |20000 |big          |
|5  |movies  |1500  |medium       |
Total rows: 5
"#,
                    );
                    continue;
                }
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

                        match apply_filter_changes_menu(
                            csv_builder,
                            &prev_iteration_builder,
                            &original_csv_builder,
                        ) {
                            Ok(_) => (),
                            Err(e) => {
                                println!("{}", e);
                                continue; // Ask for the choice again if there was an error
                            }
                        }
                    }
                    Err(e) if e.to_string() == "Operation canceled" => {
                        continue;
                    }

                    Err(e) => {
                        println!("Error getting expressions: {}", e);
                        continue;
                    }
                }
            }

            Some(3) => {
                if choice.to_lowercase() == "3d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Appends a column whose value is a concatenation of other columns. This can be useful in the following scenarios:

(1) Crating Unique Identifiers: Concatenated columns can create unique identifiers (IDs) for records when individual columns alone might not contain unique information. This is especially useful in scenarios where composite keys are required to uniquely identify records in relational databases.

(2) Granular Data Segmentation: Creating more granular category flags by appending a column that concatenates other columns can be incredibly useful in data science, particularly for enhancing data granularity, improving analysis specificity, and enabling more detailed segmentation. For instance, in a retail context, rather than analyzing all electronics together, creating flags for specific types of electronics (e.g., "laptop_high_end", "smartphone_entry_level") can reveal more nuanced consumer behavior.

{
    "new_column_name": "item_value",
    "concatenation_items": ["item", "value"]
}

|id |item    |value |item_value |
---------------------------------
|1  |books   |1000  |books1000  |
|2  |snacks  |200   |snacks200  |
|3  |cab fare|300   |cab fare300|
|4  |rent    |20000 |rent20000  |
|5  |movies  |1500  |movies1500 |
Total rows: 5
"#,
                    );
                    continue;
                }
                match get_concatenation_input() {
                    Ok((new_column_name, items_to_concatenate)) => {
                        if new_column_name.trim().is_empty() {
                            print_insight_level_2(
                                "No new column name provided. Operation aborted.",
                            );
                            continue;
                        }

                        // Convert Vec<String> to Vec<&str>
                        let items_to_concatenate_refs: Vec<&str> =
                            items_to_concatenate.iter().map(|s| s.as_str()).collect();

                        // Now pass the vector of string slices
                        csv_builder.append_derived_concatenation_column(
                            &new_column_name,
                            items_to_concatenate_refs,
                        );

                        if csv_builder.has_data() {
                            csv_builder.print_table();
                            println!();
                        }
                        print_insight_level_2("Derived concatenation column appended.");
                        match apply_filter_changes_menu(
                            csv_builder,
                            &prev_iteration_builder,
                            &original_csv_builder,
                        ) {
                            Ok(_) => (),
                            Err(e) => {
                                println!("{}", e);
                                continue; // Ask for the choice again if there was an error
                            }
                        }
                    }
                    Err(e) if e.to_string() == "Operation canceled" => {
                        continue;
                    }

                    Err(e) => {
                        println!("Error getting concatenation details: {}", e);
                        continue;
                    }
                }
            }

            Some(4) => {
                if choice.to_lowercase() == "4d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Creates category flags from columns containing data/ timestamp values.
|id |item    |value |type  |item_type     |date      |
------------------------------------------------------
|1  |books   |1000  |small |books_small   |2024-03-21|
|2  |snacks  |200   |small |snacks_small  |2024-03-22|
|3  |cab fare|300   |small |cab fare_small|2024-03-23|
|4  |rent    |20000 |big   |rent_big      |2024-03-24|
|5  |movies  |1500  |medium|movies_medium |2024-03-25|
Total rows: 5

{
    "column_name": "date",
    "date_format": "%Y-%m-%d"
}

|id |item    |value |type  |  <<+2 cols>>  |date_YEAR |date_YEAR_MONTH |date_YEAR_MONTH_DAY |
---------------------------------------------------------------------------------------------
|1  |books   |1000  |small |...            |Y2024     |Y2024-M03       |Y2024-M03-D21       |
|2  |snacks  |200   |small |...            |Y2024     |Y2024-M03       |Y2024-M03-D22       |
|3  |cab fare|300   |small |...            |Y2024     |Y2024-M03       |Y2024-M03-D23       |
|4  |rent    |20000 |big   |...            |Y2024     |Y2024-M03       |Y2024-M03-D24       |
|5  |movies  |1500  |medium|...            |Y2024     |Y2024-M03       |Y2024-M03-D25       |

Omitted columns: item_type, date
Total rows: 5

The following value formats can be processed by this feature:
- %Y-%m-%d: 2023-01-30.
- %Y-%m-%d %H:%M:%S: 2023-01-30 15:45:30.
- %Y/%m/%d: 2023/01/30
- %d-%m-%Y: 30-01-2023.
- %Y-%m-%d %H:%M:%S%.f: 2024-02-03 10:42:07.856666666
- %b %d, %Y: Jan 30, 2023.
"#,
                    );
                    continue;
                }

                match get_date_split_input() {
                    Ok((column_name, date_format)) => {
                        if column_name.trim().is_empty() || date_format.trim().is_empty() {
                            print_insight_level_2(
                                "Missing column name or date format. Operation aborted.",
                            );
                            continue;
                        }

                        csv_builder
                            .split_date_as_appended_category_columns(&column_name, &date_format);

                        if csv_builder.has_data() {
                            csv_builder.print_table();
                            println!();
                        }
                        print_insight_level_2("Date column split into category columns.");

                        match apply_filter_changes_menu(
                            csv_builder,
                            &prev_iteration_builder,
                            &original_csv_builder,
                        ) {
                            Ok(_) => (),
                            Err(e) => {
                                println!("{}", e);
                                continue; // Ask for the choice again if there was an error
                            }
                        }
                    }
                    Err(e) if e.to_string() == "Operation canceled" => {
                        continue;
                    }

                    Err(e) => {
                        println!("Error getting date split details: {}", e);
                        continue;
                    }
                }
            }

            Some(5) => {
                if choice.to_lowercase() == "5d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Creates category flags upon doing a fuzzy analysis on column values vis-a-vis specified training data.
|id |item    |value |type  |item_type     |
-------------------------------------------
|1  |books   |1000  |small |books_small   |
|2  |snacks  |200   |small |snacks_small  |
|3  |cab fare|300   |small |cab fare_small|
|4  |rent    |20000 |big   |rent_big      |
|5  |movies  |1500  |medium|movies_medium |
Total rows: 5

  @LILbro: Executing this JSON query:
{
    "column_to_analyze": "item",
    "column_prefix": "is_academic",
    "training_data": [
        {"input": "bookstore", "output": "1"},
        {"input": "library", "output": "1"},
	{"input": "food", "output": "0"},
	{"input": "house rent", "output": "0"}
    ],
    "word_split_param": "WORD_SPLIT:2",
    "word_length_sensitivity_param": "WORD_LENGTH_SENSITIVITY:0.8",
    "get_best_param": "GET_BEST:1"
}
Fuzzai analysis columns appended.

|id |item    |value |type  |  <<+1 col>>   |is_academic_rank1_fuzzai_result |is_academic_rank1_fuzzai_result_basis |is_academic_rank1_fuzzai_score |
----------------------------------------------------------------------------------------------------------------------------------------------------
|1  |books   |1000  |small |...            |1                               |bookstore                             |68.728                         |
|2  |snacks  |200   |small |...            |1                               |bookstore                             |26.352                         |
|3  |cab fare|300   |small |...            |1                               |library                               |39.68                          |
|4  |rent    |20000 |big   |...            |0                               |house rent                            |54.263999999999996             |
|5  |movies  |1500  |medium|...            |1                               |bookstore                             |26.352                         |

Omitted columns: item_type
Total rows: 5

Note the implications of the params in the JSON query:
1. "column to analyze": The name of the column to be subject to fuzzy analysis against your training data.
2. "column_prefix": The prefix of the newly created columns.
3. "training_data": Your training data. It is good practice to anticipate as many outcomes as possible.
4. "word_split_param": The word length of the smallest combination the value and the training data that would be split to ascertain the best score.
5. "word_length_sensitivity_param": Whether the fuzzy analysis score should be adjusted to give closer matches in the event the word length of the training inputs are more similar to the word length of the column value. Values can range from 0.0 to 1.0, with values closer to 1.0 resulting in higher scores where the rival words lengths are similar.
6. "get_best_param": Determines the number of fuzzy analysis results that should be provided. A value of 1 would get the best match, where as a value of 2 would also return the second best match. This can have a maximum value of 3.
"#,
                    );
                    continue;
                }

                // This matches the case in your project's workflow
                match get_fuzzai_analysis_input() {
                    Ok((
                        column_to_analyze,
                        column_prefix,
                        training_data,
                        word_split_param,
                        word_length_sensitivity_param,
                        get_best_param,
                    )) => {
                        // Assuming csv_builder is an instance of your CSV manipulation class
                        csv_builder.append_fuzzai_analysis_columns(
                            &column_to_analyze,
                            &column_prefix,
                            training_data,
                            &word_split_param,
                            &word_length_sensitivity_param,
                            &get_best_param,
                        );
                        println!("Fuzzai analysis columns appended.");

                        if csv_builder.has_data() {
                            csv_builder.print_table();
                            println!();
                        }
                        print_insight_level_2("Fuzzai Analysis columns appended.");

                        match apply_filter_changes_menu(
                            csv_builder,
                            &prev_iteration_builder,
                            &original_csv_builder,
                        ) {
                            Ok(_) => (),
                            Err(e) => {
                                println!("{}", e);
                                continue; // Ask for the choice again if there was an error
                            }
                        }
                    }
                    Err(e) if e.to_string() == "Operation canceled" => {
                        continue;
                    }

                    Err(e) => {
                        println!("Error getting fuzzai analysis details: {}", e);
                    }
                }
            }

            // 7. "expressions" and "result_expression": Indicates the exact conditions of the row, that should trigger the fuzzy analysis.
            Some(6) => {
                if choice.to_lowercase() == "6d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Creates category flags upon doing a fuzzy analysis on column values vis-a-vis specified training data, subject to the evaluation of WHERE conditions in a row.
|id |item    |value |type  |item_type     |
-------------------------------------------
|1  |books   |1000  |small |books_small   |
|2  |snacks  |200   |small |snacks_small  |
|3  |cab fare|300   |small |cab fare_small|
|4  |rent    |20000 |big   |rent_big      |
|5  |movies  |1500  |medium|movies_medium |
Total rows: 5

  @LILbro: Executing this JSON query:
{
    "column_to_analyze": "item",
    "column_prefix": "filter",
    "training_data": [
        {"input": "bookstore", "output": "yes"},
        {"input": "rentals", "output": "no"}
    ],
    "word_split_param": "WORD_SPLIT:2",
    "word_length_sensitivity_param": "WORD_LENGTH_SENSITIVITY:0.8",
    "get_best_param": "GET_BEST:2",
    "expressions": [
            [
                "Exp1",
                {
                    "column": "value",
                    "operator": ">",
                    "compare_with": "1000",
                    "compare_as": "NUMBERS"
                }
            ],
            [
                "Exp2",
                {
                    "column": "type",
                    "operator": "CONTAINS",
                    "compare_with": "big",
                    "compare_as": "TEXT"
                }
            ]
        ],
    "result_expression": "Exp1 && Exp2"
}
Fuzzai analysis columns appended.

|id |item    |value |type  |  <<+4 cols>>  |filter_rank2_fuzzai_result |filter_rank2_fuzzai_result_basis |filter_rank2_fuzzai_score |
-------------------------------------------------------------------------------------------------------------------------------------
|1  |books   |1000  |small |...            |                           |                                 |0.0                       |
|2  |snacks  |200   |small |...            |                           |                                 |0.0                       |
|3  |cab fare|300   |small |...            |                           |                                 |0.0                       |
|4  |rent    |20000 |big   |...            |yes                        |bookstore                        |29.759999999999998        |
|5  |movies  |1500  |medium|...            |                           |                                 |0.0                       |

Omitted columns: item_type, filter_rank1_fuzzai_result, filter_rank1_fuzzai_result_basis, filter_rank1_fuzzai_score
Total rows: 5

Note the implications of the params in the JSON query:
1. "column to analyze": The name of the column to be subject to fuzzy analysis against your training data.
2. "column_prefix": The prefix of the newly created columns.
3. "training_data": Your training data. It is good practice to anticipate as many outcomes as possible.
4. "word_split_param": The word length of the smallest combination the value and the training data that would be split to ascertain the best score.
5. "word_length_sensitivity_param": Whether the fuzzy analysis score should be adjusted to give closer matches in the event the word length of the training inputs are more similar to the word length of the column value. Values can range from 0.0 to 1.0, with values closer to 1.0 resulting in higher scores where the rival words lengths are similar.
6. "get_best_param": Determines the number of fuzzy analysis results that should be provided. A value of 1 would get the best match, where as a value of 2 would also return the second best match. This can have a maximum value of 3.
7. "expressions" and "result_expression": Indicates the exact conditions of the row, that should trigger the fuzzy analysis.
"#,
                    );
                    continue;
                }

                // This matches the case in your project's workflow
                match get_fuzzai_analysis_where_input() {
                    Ok((
                        column_to_analyze,
                        column_prefix,
                        training_data,
                        word_split_param,
                        word_length_sensitivity_param,
                        get_best_param,
                        expressions,
                        result_expression,
                    )) => {
                        let expressions_refs: Vec<(&str, Exp)> = expressions
                            .iter()
                            .map(|(name, exp)| (name.as_str(), exp.clone()))
                            .collect();

                        csv_builder.append_fuzzai_analysis_columns_with_values_where(
                            &column_to_analyze,
                            &column_prefix,
                            training_data,
                            &word_split_param,
                            &word_length_sensitivity_param,
                            &get_best_param,
                            expressions_refs,
                            &result_expression,
                        );
                        println!("Fuzzai analysis columns appended.");

                        if csv_builder.has_data() {
                            csv_builder.print_table();
                            println!();
                        }
                        print_insight_level_2("Fuzzai Analysis columns appended.");

                        match apply_filter_changes_menu(
                            csv_builder,
                            &prev_iteration_builder,
                            &original_csv_builder,
                        ) {
                            Ok(_) => (),
                            Err(e) => {
                                println!("{}", e);
                                continue; // Ask for the choice again if there was an error
                            }
                        }
                    }
                    Err(e) if e.to_string() == "Operation canceled" => {
                        continue;
                    }

                    Err(e) => {
                        println!("Error getting fuzzai analysis details: {}", e);
                    }
                }
            }

            Some(7) => {
                if choice.to_lowercase() == "7d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Creates category flags upon leveraging OpenAI's json mode enabled models.

IMPORTANT: IN THE EVENT THIS FEATURE DOES NOT RETURN RESULTS AS EXPECTED BELOW, YOU MAY NEED TO TRY AGAIN 1-2 MORE TIMES, AS OPEN AI API IS KNOWN TO BE "GLITCHY" NOW AND THEN. IF ISSUES PERSIST, TRY USING THE "gpt-4-0125-preview" MODEL OR A NEWER JSON-MODE COMPATIBLE MODEL, INSTEAD - AND CHECKING THE VALIDITY OF YOUR API KEY.

|id |item |description |
------------------------
|1  |books|health      |
|2  |shoes|health      |
|3  |pizza|fun         |
Total rows: 3

  @LILbro: Executing this JSON query:
{
  "target_columns": ["item", "description"],
  "analysis_query": {
    "helps_lose_weight": "a boolean value of either 1 or 0, on whether the expense has a high corelation to the user losing weight"
  },
  "model": "gpt-3.5-turbo-0125"
}

{
  "input": {
    "description": "health",
    "item": "books"
  },
  "output": {
    "helps_lose_weight": "0"
  }
}
{
  "input": {
    "description": "health",
    "item": "shoes"
  },
  "output": {
    "helps_lose_weight": "0"
  }
}
{
  "input": {
    "description": "fun",
    "item": "pizza"
  },
  "output": {
    "helps_lose_weight": "0"
  }
}

|id |item |description |helps_lose_weight |
-------------------------------------------
|1  |books|health      |0                 |
|2  |shoes|health      |0                 |
|3  |pizza|fun         |0                 |
Total rows: 3
"#,
                    );
                    continue;
                }

                match get_append_open_ai_analysis_expression() {
                    Ok((target_columns, analysis_query, model)) => {
                        // Check if the target columns are empty
                        if target_columns.is_empty() {
                            print_insight_level_2("No target columns provided. Operation aborted.");
                            continue; // Skip the rest of the process
                        }

                        //dbg!(&file_path_option);

                        let home_dir =
                            env::var("HOME").expect("Unable to determine user home directory");
                        let desktop_path = Path::new(&home_dir).join("Desktop");
                        let csv_db_path = desktop_path.join("csv_db");

                        //dbg!(&csv_db_path);

                        let config_path = PathBuf::from(csv_db_path).join("bro.config");

                        let file_contents = read_to_string(config_path)?;
                        let valid_json_part = file_contents
                            .split("SYNTAX")
                            .next()
                            .ok_or("Invalid configuration format")?;
                        let config: Config = from_str(valid_json_part)?;
                        let api_key = &config.open_ai_key;

                        // Use the api_key for your needs
                        //println!("API Key: {}", api_key);

                        // Convert target_columns to Vec<&str>
                        let target_columns_refs: Vec<&str> =
                            target_columns.iter().map(String::as_str).collect();
                        println!();
                        let result = csv_builder
                            .append_derived_openai_analysis_columns(
                                target_columns_refs,
                                analysis_query,
                                api_key,
                                &model,
                            )
                            .await;

                        if result.has_data() {
                            csv_builder.print_table();
                            println!();
                            print_insight_level_2("OpenAI analysis complete.");
                        }

                        match apply_filter_changes_menu(
                            csv_builder,
                            &prev_iteration_builder,
                            &original_csv_builder,
                        ) {
                            Ok(_) => (),
                            Err(e) => {
                                println!("{}", e);
                                continue; // Ask for the choice again if there was an error
                            }
                        }
                    }
                    Err(e) if e.to_string() == "Operation canceled" => {
                        continue;
                    }

                    Err(e) => {
                        println!("Error getting expressions: {}", e);
                        continue; // Return to the menu to let the user try again or choose another option
                    }
                }
            }

            Some(8) => {
                if choice.to_lowercase() == "8d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Studies your training data, and makes multi-dimensional linear regression predictions against numerical and text format column values (leveraging the Levenshtein distance as a normalizer for comparisons involving text, and traditional linear regression computation for numerical values).

IMPORTANT: THE NUMBER OF TRAINING EXAMPLES SHOULD BE AT LEAST 2X THE NUMBER OF UNIQUE TRAINING OUTPUTS. IN THE BELOW EXAMPLE, THERE ARE TWO UNIQUE TRAINING OUTPUTS (10 AND 90), AND TEN TRAINING EXAMPLES.

|id |item    |value |type  |item_type     |
-------------------------------------------
|1  |books   |1000  |small |books_small   |
|2  |snacks  |200   |small |snacks_small  |
|3  |cab fare|300   |small |cab fare_small|
|4  |rent    |20000 |big   |rent_big      |
|5  |movies  |1500  |medium|movies_medium |
Total rows: 5

  @LILbro: Executing this JSON query:
{
  "new_column_name": "is_splurge",
  "output_range": [0, 100],
  "training_data": [
   [
     "90",
     {
       "item": "rent",
       "value": "50000"
     }
   ],
   [
     "90",
     {
       "item": "snacks",
       "value": "200"
     }
   ],
   [
     "90",
     {
       "item": "snacks",
       "value": "150"
     }
   ],
   [
     "90",
     {
       "item": "books",
       "value": "5000"
     }
   ],
   [
     "90",
     {
       "item": "books",
       "value": "4000"
     }
   ],
   [
     "10",
     {
       "item": "rent",
       "value": "15000"
     }
   ],
   [
     "10",
     {
       "item": "snacks",
       "value": "10"
     }
   ],
   [
     "10",
     {
       "item": "snacks",
       "value": "15"
     }
   ],
   [
     "10",
     {
       "item": "books",
       "value": "500"
     }
   ],
   [
     "10",
     {
       "item": "books",
       "value": "200"
     }
   ]
  ]
}

|id |item    |value |type  |item_type     |is_splurge        |
--------------------------------------------------------------
|1  |books   |1000  |small |books_small   |49.294082425077534|
|2  |snacks  |200   |small |snacks_small  |47.1167944831484  |
|3  |cab fare|300   |small |cab fare_small|100               |
|4  |rent    |20000 |big   |rent_big      |15.979875907356927|
|5  |movies  |1500  |medium|movies_medium |100               |
Total rows: 5
"#,
                    );
                    continue;
                }

                match get_append_linear_regression_expression() {
                    Ok((
                        new_column_name,
                        training_predictors,
                        training_outputs,
                        output_range,
                        test_predictors_column_names,
                    )) => {
                        // Check if the new column name is empty
                        if new_column_name.trim().is_empty() {
                            print_insight_level_2(
                                "No new column name provided. Operation aborted.",
                            );
                            continue; // Skip the rest of the process
                        }

                        // Sort and deduplicate training_outputs manually
                        let mut sorted_outputs = training_outputs.clone();
                        sorted_outputs.sort_by(|a, b| a.partial_cmp(b).unwrap());
                        sorted_outputs.dedup(); // This removes consecutive duplicate elements

                        //dbg!(&training_predictors.len(), &sorted_outputs.len());

                        // Verify the condition
                        if training_predictors.len() < 2 * sorted_outputs.len() {
                            print_insight_level_2("Insufficient training predictors: There must be at least twice as many predictor rows as unique outputs.");
                            continue; // Skip the rest of the process
                        }

                        // Append the new derived linear regression column
                        csv_builder.append_derived_linear_regression_column(
                            &new_column_name,
                            training_predictors,
                            training_outputs,
                            output_range,
                            test_predictors_column_names,
                        );
                        if csv_builder.has_data() {
                            csv_builder.print_table();
                            println!();
                        }
                        print_insight_level_2("Derived linear regression column appended.");

                        match apply_filter_changes_menu(
                            csv_builder,
                            &prev_iteration_builder,
                            &original_csv_builder,
                        ) {
                            Ok(_) => (),
                            Err(e) => {
                                println!("{}", e);
                                continue; // Ask for the choice again if there was an error
                            }
                        }
                    }
                    Err(e) if e.to_string() == "Operation canceled" => {
                        continue;
                    }

                    Err(e) => {
                        println!("Error getting expressions: {}", e);
                        continue; // Return to the menu to let the user try again or choose another option
                    }
                }
            }

            Some(9) => {
                if choice.to_lowercase() == "9d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Creates a pivot table indexed at a category label (accruing from the unique values in a category column).

Example 1:
----------
  @LILbro: Executing this JSON query:
{
    "index_at": "date_YEAR_MONTH",
    "values_from": "value",
    "operation": "SUM",
    "seggregate_by": [],
    "save_as": ""
}

|id |item    |value |type  |  <<+2 cols>>  |date_YEAR |date_YEAR_MONTH |date_YEAR_MONTH_DAY |
---------------------------------------------------------------------------------------------
|1  |books   |1000  |small |...            |Y2024     |Y2024-M01       |Y2024-M01-D21       |
|2  |snacks  |200   |small |...            |Y2024     |Y2024-M02       |Y2024-M02-D22       |
|3  |cab fare|300   |small |...            |Y2024     |Y2024-M03       |Y2024-M03-D23       |
|4  |rent    |20000 |big   |...            |Y2024     |Y2024-M01       |Y2024-M01-D24       |
|5  |movies  |1500  |medium|...            |Y2024     |Y2024-M02       |Y2024-M02-D25       |
|6  |books   |1000  |small |...            |Y2024     |Y2024-M03       |Y2024-M03-D21       |
|7  |snacks  |200   |small |...            |Y2024     |Y2024-M01       |Y2024-M01-D22       |
|8  |cab fare|300   |small |...            |Y2024     |Y2024-M02       |Y2024-M02-D23       |
|9  |rent    |20000 |big   |...            |Y2024     |Y2024-M03       |Y2024-M03-D24       |
|10 |movies  |1500  |medium|...            |Y2024     |Y2024-M01       |Y2024-M01-D25       |

Omitted columns: item_type, date
Total rows: 10


|Index    |Value   |
--------------------
|Y2024-M01|22700.00|
|Y2024-M02|2000.00 |
|Y2024-M03|21300.00|
Total rows: 3
Temporary file deleted successfully.

Example 2:
----------
  @LILbro: Executing this JSON query:
{
    "index_at": "date_YEAR_MONTH",
    "values_from": "value",
    "operation": "SUM",
    "seggregate_by": [
        {"column": "relates_to_travel", "type": "AS_BOOLEAN"},
        {"column": "type", "type": "AS_CATEGORY"}
    ],
    "save_as": ""
}

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


|Index    |relates_to_travel |FOOD  |OTHER   |TRAVEL |relates_to_travel |Value   |
----------------------------------------------------------------------------------
|Y2024-M01|0.00              |200.00|22500.00|0.00   |0.00              |22700.00|
|Y2024-M02|300.00            |200.00|1500.00 |300.00 |300.00            |2000.00 |
|Y2024-M03|300.00            |0.00  |21000.00|300.00 |300.00            |21300.00|
Total rows: 3
Temporary file deleted successfully.

Note the implication of params in the Json Query:
1. "index_at": This parameter determines the primary key column of the pivot table, or the field by which the data will be grouped vertically (row labels). It's the main dimension of analysis. This can be either a text or a number, depending on the data you are grouping by. For example, if you are grouping sales data by region, index_at could be the name of the region (text). If you are grouping by year, it could be the year (number).
2. "values_from": Specifies the column(s) from which to retrieve the values that will be summarized or aggregated in the pivot table. This would be a column with numerical data since you are usually performing operations like sums, averages, counts, etc.
3. "operation": Defines the type of aggregation or summarization to perform on the values_from data across the grouped index_at categories. Operations include "COUNT", "COUNT_UNIQUE", "SUM", "MEAN", "BOOL_PERCENT" (assuming column values of 0 or 1 in 'values_from', calculates the % of 1 values for the segment)
4. "seggregate_by": This parameter allows for additional segmentation of data within the primary grouping defined by index_at. Each segment within seggregate_by can further divide the data based on the specified column and the type of segmentation (like categorical grouping or binning numerical data into ranges).
- 4.1. Column: Can be both text or number, similar to index_at, depending on what additional dimension you want to segment the data by.
- 4.2. Type: Is text, indicating how the segmentation should be applied. The column specified can have a type of "AS_CATEGORY", or "AS_BOOLEAN"
  - 4.2.1. AS_CATEGORY: It means that each unique value in the specified seggregation column will create a separate subgroup within each primary group. This is appropriate for text data or numerical data that represent distinct categories or groups rather than values to be aggregated.
  - 4.2.2. AS_BOOLEAN: By setting the type to "AS_BOOLEAN", it's understood that the specified seggregation column contains boolean values (1/0). The data will be segmented into two groups based on these boolean values. This type is particularly useful for flag columns that indicate the presence or absence of a particular condition or attribute.
"#,
                    );
                    continue;
                }

                // This matches the case in your project's workflow for the pivot operation
                match get_pivot_input() {
                    Ok((piv, save_as_path)) => {
                        // Get the user's home directory or panic if not found
                        let home_dir =
                            env::var("HOME").expect("Unable to determine user home directory");
                        let desktop_path = Path::new(&home_dir).join("Desktop");
                        let csv_db_path = desktop_path.join("csv_db");
                        let default_csv_path = desktop_path.join("csv_db/temp_pivot_file.csv");

                        // Determine the final path based on whether `save_as_path` is provided
                        let final_path = if save_as_path.is_empty() {
                            default_csv_path.clone()
                        } else {
                            csv_db_path.join(&save_as_path)
                        };

                        // Ensure the final path is valid Unicode
                        let final_path_str = final_path
                            .to_str()
                            .expect("Path contains invalid Unicode characters");

                        // Determine the full file name, appending `.csv` if necessary
                        let full_file_name = if final_path_str.ends_with(".csv") {
                            final_path_str.to_string()
                        } else {
                            format!("{}.csv", final_path_str)
                        };

                        csv_builder.print_table().pivot_as(&full_file_name, piv);
                        println!();

                        // If 'save_as_path' is not empty, use it to create and print from the CsvBuilder object
                        if !save_as_path.is_empty() {
                            let _ = CsvBuilder::from_csv(&full_file_name).print_table_all_rows();
                            //.save_as(&full_file_name);
                            println!();
                            print_insight_level_2(&format!("CSV file saved at {}", full_file_name));
                        } else {
                            // If 'save_as_path' is empty, assume the pivot operation used the default temp path
                            // Create a CsvBuilder object from the temp file and print
                            CsvBuilder::from_csv(default_csv_path.to_str().unwrap())
                                .print_table_all_rows();

                            // Delete the temporary file after printing
                            if let Err(e) = fs::remove_file(default_csv_path) {
                                println!("Failed to delete temporary file: {}", e);
                            } else {
                                println!("Temporary file deleted successfully.");
                            }
                        }
                    }
                    Err(e) if e.to_string() == "Operation canceled" => {
                        continue;
                    }

                    Err(e) => println!("Error getting pivot details: {}", e),
                }
            }

            _ => {
                println!("Invalid option. Please enter a number from 1 to 9.");
                continue; // Ask for the choice again
            }
        }

        println!(); // Print a new line for better readability
    }

    Ok(())
}
