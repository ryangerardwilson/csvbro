// csv_pivoter.rs
use crate::settings::manage_open_ai_config_file;
use crate::user_interaction::{
    determine_action_as_number, get_edited_user_json_input, get_user_input_level_2,
    print_insight_level_2, print_list_level_2,
};
use rgwml::csv_utils::{CsvBuilder, Exp, ExpVal, Piv, Train};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

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

pub async fn handle_pivot(csv_builder: &mut CsvBuilder) -> Result<(), Box<dyn std::error::Error>> {
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
    ) -> Result<(String, HashMap<String, String>, String), Box<dyn Error>> {
        let syntax = r#"{
  "target_column_name": "",
  "analysis_query": {
    "": "",
    "": ""
  },
  "model": "gpt-3.5-turbo-0125"
}

SYNTAX
======

{
  "target_column_name": "transcribed_text",
  "analysis_query": {
    "customer_query": "extract the gist of the query raised by customer in the conversation text",
    "agent_response": "extract the gist of the response given by agent to customer in the conversation text"
  },
  "model": "gpt-3.5-turbo-0125" // Other compatible models inlcude "gpt-4-turbo-preview"
}

  "#;

        let exp_json = get_edited_user_json_input((&syntax).to_string());

        //dbg!(&exp_json);

        let parsed_json: Value = serde_json::from_str(&exp_json)?;

        //dbg!(&parsed_json);

        let target_column_name = parsed_json["target_column_name"]
            .as_str()
            .unwrap_or_default()
            .to_string();

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

        Ok((target_column_name, analysis_query, model))
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

    "#;

        // Assume get_edited_user_json_input allows user to edit the predefined syntax
        let fuzzai_json = get_edited_user_json_input(syntax.to_string());
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

    "#;

        // Assume get_edited_user_json_input allows user to edit the predefined syntax
        let fuzzai_json = get_edited_user_json_input(syntax.to_string());
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
    "operation": "MEDIAN", // Also "COUNT", "SUM", "MEAN", "BOOL_PERCENT" (assuming column values of 0 or 1 in 'values_from', calculates the % of 1 values for the segment)
    "seggregate_by": [  // Leave as empty [] if seggregation is not required
        {"column": "Category", "type": "AS_CATEGORY"},
        {"column": "IsPromotion", "type": "AS_BOOLEAN"}
    ],
    "save_as": "analysis1" // Leave as "" if you dont want to save it
}


"#;

        let user_input = get_edited_user_json_input(pivot_syntax.to_string());
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
        "GO BACK",
    ];

    loop {
        print_insight_level_2("Select an option to inspect CSV data:");
        print_list_level_2(&menu_options);

        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        let selected_option = determine_action_as_number(&menu_options, &choice);

        match selected_option {
            Some(1) => {


        if choice.to_lowercase() == "1d" {
            print_insight_level_2(r#"Documentation

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

"#);
        } else {


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



            }

            Some(2) => {


        if choice.to_lowercase() == "2d" {
            print_insight_level_2(r#"DOCUMENTATION

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

"#);
        } else {



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
}

            Some(3) => {
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
                    }
                    Err(e) => {
                        println!("Error getting concatenation details: {}", e);
                        continue;
                    }
                }
            }

            Some(4) => match get_date_split_input() {
                Ok((column_name, date_format)) => {
                    if column_name.trim().is_empty() || date_format.trim().is_empty() {
                        print_insight_level_2(
                            "Missing column name or date format. Operation aborted.",
                        );
                        continue;
                    }

                    csv_builder.split_date_as_appended_category_columns(&column_name, &date_format);

                    if csv_builder.has_data() {
                        csv_builder.print_table();
                        println!();
                    }
                    print_insight_level_2("Date column split into category columns.");
                }
                Err(e) => {
                    println!("Error getting date split details: {}", e);
                    continue;
                }
            },

            Some(5) => {
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
                    }
                    Err(e) => {
                        println!("Error getting fuzzai analysis details: {}", e);
                    }
                }
            }

            Some(6) => {
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
                    }
                    Err(e) => {
                        println!("Error getting fuzzai analysis details: {}", e);
                    }
                }
            }

            Some(7) => {
                match get_append_open_ai_analysis_expression() {
                    Ok((target_column_name, analysis_query, model)) => {
                        // Check if the new column name is empty
                        if target_column_name.trim().is_empty() {
                            print_insight_level_2(
                                "No target column name provided. Operation aborted.",
                            );
                            continue; // Skip the rest of the process
                        }

                        let mut presets = Vec::new();
                        let _ = manage_open_ai_config_file(|config| {
                            presets = config.open_ai_presets.clone(); // Assign the presets here
                            Ok(()) // Return Ok(()) as expected by the function signature
                        });

                        let api_key = &presets[0].api_key;

                        //dbg!(&presets, &api_key);

                        let result = csv_builder
                            .append_derived_openai_analysis_columns(
                                &target_column_name,
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
                    }
                    Err(e) => {
                        println!("Error getting expressions: {}", e);
                        continue; // Return to the menu to let the user try again or choose another option
                    }
                }
            }

            Some(8) => {
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
                    }
                    Err(e) => {
                        println!("Error getting expressions: {}", e);
                        continue; // Return to the menu to let the user try again or choose another option
                    }
                }
            }

            Some(9) => {
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

                    Err(e) => println!("Error getting pivot details: {}", e),
                }
            }


            Some(10) => {
                csv_builder.print_table();
                break; // Exit the inspect handler
            }
            _ => {
                println!("Invalid option. Please enter a number from 1 to 10.");
                continue; // Ask for the choice again
            }
        }

        println!(); // Print a new line for better readability
    }

    Ok(())
}
