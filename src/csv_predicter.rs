// csv_predicter.rs
use crate::user_experience::{handle_back_flag, handle_cancel_flag};
use crate::user_interaction::{
    get_edited_user_json_input, get_user_input_level_2, print_insight_level_2, print_list_level_2,
};
use rgwml::csv_utils::{CsvBuilder, Exp, ExpVal};
use rgwml::xgb_utils::{XgbConfig, XgbConnect};
use serde_json::to_string_pretty;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

pub async fn handle_predict(
    mut csv_builder: CsvBuilder,
    _file_path_option: Option<&str>,
    action_type: &str,
    action_feature: &str,
    action_flag: &str,
    action_menu_options: Vec<&str>,
    big_file_threshold: &str,
) -> Result<(CsvBuilder, bool), Box<dyn std::error::Error>> {
    fn get_xgb_model_input(
    ) -> Result<(String, String, String, String, XgbConfig), Box<dyn std::error::Error>> {
        let xgb_model_input_syntax = r#"{
    "param_columns": "",
    "target_column": "",
    "prediction_column_name": "",
    "save_model_as": "",
    "xgb_config": {
        "xgb_objective": "",
        "xgb_max_depth": "",
        "xgb_learning_rate": "",
        "xgb_n_estimators": "",
        "xgb_gamma": "",
        "xgb_min_child_weight": "",
        "xgb_subsample": "",
        "xgb_colsample_bytree": "",
        "xgb_reg_lambda": "",
        "xgb_reg_alpha": "",
        "xgb_scale_pos_weight": "",
        "xgb_max_delta_step": "",
        "xgb_booster": "",
        "xgb_tree_method": "",
        "xgb_grow_policy": "",
        "xgb_eval_metric": "",
        "xgb_early_stopping_rounds": "",
        "xgb_device": "",
        "xgb_cv": "",
        "xgb_interaction_constraints": "",
        "hyperparameter_optimization_attempts": "",
        "hyperparameter_optimization_result_display_limit": "",
        "dask_workers": "",
        "dask_threads_per_worker": ""
    }
}

SYNTAX
======

Default presets in the event the xgb_config is left blank, shall operate as below:

{
    "param_columns": "", // Required, Comma-separated column names to use as parameters for model training
    "target_column": "", // Optional, Name of the target column
    "prediction_column_name": "", // Required, Name of the prediction column
    "save_model_as": "", // Optional, Specify the name of the trained model
    "xgb_config": {     
        "xgb_objective": "binary:logistic", // Use "reg:squarederror" for linear regression
        "xgb_max_depth": 6, // Maximum tree depth for base learners
        "xgb_learning_rate": 0.05, // Boosting learning rate
        "xgb_n_estimators": 200, // Number of boosting rounds
        "xgb_gamma": 0.0, // Minimum loss reduction required to make a further partition on a leaf node
        "xgb_min_child_weight": 1, // Minimum sum of instance weight (hessian) needed in a child
        "xgb_subsample": 0.8, // Subsample ratio of the training instances
        "xgb_colsample_bytree": 0.8, // Subsample ratio of columns when constructing each tree
        "xgb_reg_lambda": 1.0, // L2 regularization term on weights
        "xgb_reg_alpha": 0.0, // L1 regularization term on weights
        "xgb_scale_pos_weight": 1.0, // Balancing of positive and negative weights
        "xgb_max_delta_step": 0.0, // Maximum delta step we allow each tree’s weight estimation to be
        "xgb_booster": "gbtree", // Which booster to use
        "xgb_tree_method": "auto", // Specify the tree construction algorithm used in XGBoost
        "xgb_grow_policy": "depthwise", // Controls a way new nodes are added to the tree
        "xgb_eval_metric": "rmse", // Evaluation metric for validation data
        "xgb_early_stopping_rounds": 10, // Activates early stopping. Validation metric needs to improve at least once in every *early_stopping_rounds* round(s) to continue training
        "xgb_device": "cpu", // Device to run the training on (e.g., "cpu", "cuda")
        "xgb_cv": 5, // Number of cross-validation folds, default 5
        "xgb_interaction_constraints": "", // Constraints for interaction between variables
        "hyperparameter_optimization_attempts": 0, // Set to above 0 to activate
        "hyperparameter_optimization_result_display_limit": 3, // Adjust this to change how many rankings of hyperparameter optimizations are returned
        "dask_workers": 4, // Number of dask workers
        "dask_threads_per_worker": 1 // Number of threads per dask worker
    }
}
"#;

        let user_input = get_edited_user_json_input(xgb_model_input_syntax.to_string());

        if handle_cancel_flag(&user_input) {
            return Err("Operation canceled".into());
        }

        let parsed_json: Value = serde_json::from_str(&user_input)?;

        let param_columns = parsed_json["param_columns"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let target_column = parsed_json["target_column"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let prediction_column_name = parsed_json["prediction_column_name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let save_model_as = parsed_json["save_model_as"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let xgb_config = &parsed_json["xgb_config"];

        let config = XgbConfig {
            xgb_objective: xgb_config["xgb_objective"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_max_depth: xgb_config["xgb_max_depth"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_learning_rate: xgb_config["xgb_learning_rate"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_n_estimators: xgb_config["xgb_n_estimators"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_gamma: xgb_config["xgb_gamma"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_min_child_weight: xgb_config["xgb_min_child_weight"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_subsample: xgb_config["xgb_subsample"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_colsample_bytree: xgb_config["xgb_colsample_bytree"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_reg_lambda: xgb_config["xgb_reg_lambda"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_reg_alpha: xgb_config["xgb_reg_alpha"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_scale_pos_weight: xgb_config["xgb_scale_pos_weight"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_max_delta_step: xgb_config["xgb_max_delta_step"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_booster: xgb_config["xgb_booster"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_tree_method: xgb_config["xgb_tree_method"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_grow_policy: xgb_config["xgb_grow_policy"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_eval_metric: xgb_config["xgb_eval_metric"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_early_stopping_rounds: xgb_config["xgb_early_stopping_rounds"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_device: xgb_config["xgb_device"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_cv: xgb_config["xgb_cv"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            xgb_interaction_constraints: xgb_config["xgb_interaction_constraints"]
                .as_str()
                .unwrap_or_default()
                .to_string(),

            hyperparameter_optimization_attempts: xgb_config
                ["hyperparameter_optimization_attempts"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            hyperparameter_optimization_result_display_limit: xgb_config
                ["hyperparameter_optimization_result_display_limit"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            dask_workers: xgb_config["dask_workers"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            dask_threads_per_worker: xgb_config["dask_threads_per_worker"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        };

        Ok((
            param_columns,
            target_column,
            prediction_column_name,
            save_model_as,
            config,
        ))
    }

    pub fn delete_xgb_file(csv_db_path: &PathBuf) {
        fn list_xgb_files(path: &PathBuf) -> io::Result<Vec<PathBuf>> {
            let mut files = Vec::new();
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                    files.push(path);
                }
            }
            Ok(files)
        }

        fn parse_ranges(range_str: &str) -> Vec<usize> {
            range_str
                .split(',')
                .flat_map(|part| {
                    let part = part.trim();
                    if part.contains('-') {
                        let bounds: Vec<&str> = part.split('-').map(str::trim).collect();
                        if bounds.len() == 2 {
                            let start = bounds[0].parse::<usize>().unwrap_or(0);
                            let end = bounds[1].parse::<usize>().unwrap_or(0);
                            (start..=end).collect::<Vec<usize>>()
                        } else {
                            vec![]
                        }
                    } else {
                        vec![part.parse::<usize>().unwrap_or(0)]
                    }
                })
                .collect()
        }

        let models_path = csv_db_path.join("xgb_models");
        let xgb_models_path_str = models_path.to_str().unwrap();

        let mut xgb_models_builder =
            XgbConnect::get_all_xgb_models(xgb_models_path_str).expect("Failed to load XGB models");

        //let models_path = csv_db_path.join("xgb_models");
        xgb_models_builder
            .add_column_header("id")
            .order_columns(vec!["id", "..."])
            .cascade_sort(vec![("last_modified".to_string(), "ASC".to_string())])
            .resequence_id_column("id")
            .print_table_all_rows();
        println!();

        // Extract IDs and corresponding file names from xgb_models_builder
        let binding = Vec::new();
        let data = xgb_models_builder.get_data().unwrap_or(&binding);
        let id_to_file_map: HashMap<usize, &str> = data
            .iter()
            .map(|row| {
                let id = row[0].parse::<usize>().unwrap_or(0);
                let file_name = &row[1];
                (id, file_name.as_str())
            })
            .collect();

        loop {
            match list_xgb_files(&models_path) {
                Ok(files) => {
                    if files.is_empty() {
                        println!("No files in sight, bro.");
                        return;
                    }

                    let choice = get_user_input_level_2(
                        "Enter the IDs of the models to delete, separated by commas: ",
                    )
                    .trim()
                    .to_lowercase();

                    if handle_back_flag(&choice) || handle_cancel_flag(&choice) {
                        return;
                    }

                    let mut indices = parse_ranges(&choice);
                    indices.sort();
                    indices.reverse();

                    for index in indices {
                        if let Some(file_name) = id_to_file_map.get(&index) {
                            let file_path = files.iter().find(|&file| {
                                file.file_name()
                                    .and_then(|n| n.to_str())
                                    .map_or(false, |n| n == *file_name)
                            });

                            if let Some(file_path) = file_path {
                                if file_path.is_file() {
                                    print_insight_level_2(&format!("Deleting {}", file_name));
                                    if let Err(e) = fs::remove_file(file_path) {
                                        print_insight_level_2(&format!(
                                            "Failed to delete file: {}",
                                            e
                                        ));
                                    } else {
                                        print_insight_level_2("File deleted successfully.");
                                    }
                                }
                            } else {
                                print_insight_level_2("File not found for the provided ID.");
                            }
                        } else {
                            print_insight_level_2("Invalid ID provided.");
                        }
                    }

                    let mut xgb_models_builder_2 =
                        XgbConnect::get_all_xgb_models(xgb_models_path_str)
                            .expect("Failed to load XGB models");

                    //let models_path = csv_db_path.join("xgb_models");
                    xgb_models_builder_2
                        .add_column_header("id")
                        .order_columns(vec!["id", "..."])
                        .cascade_sort(vec![("last_modified".to_string(), "ASC".to_string())])
                        .resequence_id_column("id")
                        .print_table_all_rows();
                    println!();
                }
                Err(_) => {
                    print_insight_level_2("Failed to read the directory.");
                    return;
                }
            }
        }
    }

    pub fn get_xgb_details(csv_db_path: &PathBuf) -> io::Result<(PathBuf, String)> {
        fn list_xgb_files(path: &PathBuf) -> io::Result<Vec<PathBuf>> {
            let mut files = Vec::new();
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                    files.push(path);
                }
            }
            Ok(files)
        }

        fn parse_ranges(range_str: &str) -> Vec<usize> {
            range_str
                .split(',')
                .flat_map(|part| {
                    let part = part.trim();
                    if part.contains('-') {
                        let bounds: Vec<&str> = part.split('-').map(str::trim).collect();
                        if bounds.len() == 2 {
                            let start = bounds[0].parse::<usize>().unwrap_or(0);
                            let end = bounds[1].parse::<usize>().unwrap_or(0);
                            (start..=end).collect::<Vec<usize>>()
                        } else {
                            vec![]
                        }
                    } else {
                        vec![part.parse::<usize>().unwrap_or(0)]
                    }
                })
                .collect()
        }

        let models_path = csv_db_path.join("xgb_models");
        let xgb_models_path_str = models_path.to_str().unwrap();

        let mut xgb_models_builder =
            XgbConnect::get_all_xgb_models(xgb_models_path_str).expect("Failed to load XGB models");

        xgb_models_builder
            .add_column_header("id")
            .order_columns(vec!["id", "..."])
            .cascade_sort(vec![("last_modified".to_string(), "ASC".to_string())])
            .resequence_id_column("id")
            .print_table_all_rows();
        println!();

        let binding = Vec::new();
        let data = xgb_models_builder.get_data().unwrap_or(&binding);
        let id_to_file_map: HashMap<usize, &str> = data
            .iter()
            .map(|row| {
                let id = row[0].parse::<usize>().unwrap_or(0);
                let file_name = &row[1];
                (id, file_name.as_str())
            })
            .collect();

        loop {
            match list_xgb_files(&models_path) {
                Ok(files) => {
                    if files.is_empty() {
                        println!("No files in sight, bro.");
                        return Err(io::Error::new(io::ErrorKind::NotFound, "No files found"));
                    }

                    let choice = get_user_input_level_2(
                    "Enter the IDs of the models to retrieve details for, separated by commas: ",
                )
                .trim()
                .to_lowercase();

                    if handle_back_flag(&choice) || handle_cancel_flag(&choice) {
                        return Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "Operation canceled by user",
                        ));
                    }

                    let indices = parse_ranges(&choice);

                    for index in indices {
                        if let Some(file_name) = id_to_file_map.get(&index) {
                            let file_path = files.iter().find(|&file| {
                                file.file_name()
                                    .and_then(|n| n.to_str())
                                    .map_or(false, |n| n == *file_name)
                            });

                            if let Some(file_path) = file_path {
                                if file_path.is_file() {
                                    // Using the helper function to get the "params" column value
                                    let params = xgb_models_builder
                                        .where_(
                                            vec![(
                                                "Exp1",
                                                Exp {
                                                    column: "id".to_string(),
                                                    operator: "==".to_string(),
                                                    compare_with: ExpVal::STR(index.to_string()),
                                                    compare_as: "TEXT".to_string(), // Also: "NUMBERS", "TIMESTAMPS"
                                                },
                                            )],
                                            "Exp1",
                                        )
                                        .get("parameters");
                                    return Ok((file_path.to_path_buf(), params));
                                }
                            } else {
                                print_insight_level_2("File not found for the provided ID.");
                            }
                        } else {
                            print_insight_level_2("Invalid ID provided.");
                        }
                    }

                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "No valid file found for the provided IDs",
                    ));
                }
                Err(_) => {
                    print_insight_level_2("Failed to read the directory.");
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Failed to read the directory",
                    ));
                }
            }
        }
    }

    match action_feature {
        "" => {
            let action_sub_menu_options = vec![
                "APPEND XGB_TYPE LABEL COLUMN BY RATIO",
                "CREATE XGB MODEL",
                "LIST XGB MODELS",
                "DELETE XGB MODELS",
                "APPEND XGB MODEL PREDICTIONS COLUMN",
            ];

            print_list_level_2(&action_menu_options, &action_sub_menu_options, &action_type);

            return Ok((csv_builder, false));
        }

        "1" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Appends a XGB_TYPE model column labelling rows as TRAIN, VALIDATE, or TEST, as per the ratio provided.

"#,
                );
                return Ok((csv_builder, false));
            }

            let xgb_ratio_str = get_user_input_level_2(
                "Enter the TRAIN:VALIDATE:TEST ratio to label data by (for instance: 70:20:10): ",
            );

            if handle_cancel_flag(&xgb_ratio_str) {
                return Ok((csv_builder, false));
            }

            csv_builder.append_xgb_label_by_ratio_column(&xgb_ratio_str);

            if csv_builder.has_data() {
                csv_builder.print_table(&big_file_threshold).await;
                println!();
            }
        }

        "2" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Creates an XGB Model.

# 1. Binary Classification Example
-------------------------------

|XGB_TYPE |target |feature1 |feature2 |feature3 |
-------------------------------------------------
|TRAIN    |1      |5.1      |3.5      |1.4      |
|TRAIN    |0      |4.9      |3.0      |1.4      |
|TRAIN    |1      |4.7      |3.2      |1.3      |
|TRAIN    |0      |4.6      |3.1      |1.5      |
|TRAIN    |1      |5.8      |3.8      |1.6      |
<<+26 rows>>
|TEST     |0      |5.2      |3.4      |1.4      |
|TEST     |1      |5.8      |3.1      |1.7      |
|TEST     |0      |5.0      |3.2      |1.5      |
|TEST     |1      |5.9      |3.3      |1.7      |
|TEST     |0      |5.3      |3.0      |1.4      |
Total rows: 36

  @LILbro: Executing this JSON query:
{
    "param_columns": "feature1, feature2, feature3",
    "target_column": "target",
    "prediction_column_name": "target_PREDICTION",
    "save_model_as": "test_binary_classification",
    "xgb_config": {
        "objective": "binary:logistic",
        "max_depth": "",
        "learning_rate": "",
        "n_estimators": "",
        "gamma": "",
        "min_child_weight": "",
        "subsample": "",
        "colsample_bytree": "",
        "reg_lambda": "",
        "reg_alpha": "",
        "scale_pos_weight": "",
        "max_delta_step": "",
        "booster": "",
        "tree_method": "",
        "grow_policy": "",
        "eval_metric": "",
        "early_stopping_rounds": "",
        "device": "",
        "cv": "",
        "interaction_constraints": ""
    }
}

|XGB_TYPE |target |feature1 |feature2 |feature3 |target_PREDICTION |
--------------------------------------------------------------------
|TRAIN    |1      |5.1      |3.5      |1.4      |                  |
|TRAIN    |0      |4.9      |3.0      |1.4      |                  |
|TRAIN    |1      |4.7      |3.2      |1.3      |                  |
|TRAIN    |0      |4.6      |3.1      |1.5      |                  |
|TRAIN    |1      |5.8      |3.8      |1.6      |                  |
<<+26 rows>>
|TEST     |0      |5.2      |3.4      |1.4      |1                 |
|TEST     |1      |5.8      |3.1      |1.7      |0                 |
|TEST     |0      |5.0      |3.2      |1.5      |0                 |
|TEST     |1      |5.9      |3.3      |1.7      |1                 |
|TEST     |0      |5.3      |3.0      |1.4      |0                 |
Total rows: 36

  @LILBro: Yo, here's the lowdown on the data training:
{
  "0": {
    "f1-score": 0.5454545454545454,
    "precision": 0.5,
    "recall": 0.6,
    "support": 5.0
  },
  "1": {
    "f1-score": 0.4444444444444444,
    "precision": 0.5,
    "recall": 0.4,
    "support": 5.0
  },
  "accuracy": 0.5,
  "macro avg": {
    "f1-score": 0.4949494949494949,
    "precision": 0.5,
    "recall": 0.5,
    "support": 10.0
  },
  "weighted avg": {
    "f1-score": 0.494949494949495,
    "precision": 0.5,
    "recall": 0.5,
    "support": 10.0
  }
}

# 2. Linear Regression Example
------------------------------

|XGB_TYPE |no_of_tickets |last_60_days_tickets |churn_day |
-----------------------------------------------------------
|TRAIN    |5             |2                    |180       |
|TRAIN    |6             |3                    |170       |
|TRAIN    |4             |1                    |190       |
|TRAIN    |5             |1                    |185       |
|TRAIN    |10            |6                    |90        |
<<+10 rows>>
|TEST     |6             |2                    |173       |
|TEST     |13            |6                    |68        |
|TEST     |12            |8                    |69        |
|TEST     |22            |9                    |66        |
|TEST     |32            |9                    |46        |
Total rows: 20

  @LILbro: Executing this JSON query:
{
    "param_columns": "no_of_tickets, last_60_days_tickets",
    "target_column": "churn_day",
    "prediction_column_name": "churn_day_predictions",
    "save_model_as": "test_regression_model",
    "xgb_config": {
        "objective": "reg:squarederror",
        "max_depth": "",
        "learning_rate": "",
        "n_estimators": "",
        "gamma": "",
        "min_child_weight": "",
        "subsample": "",
        "colsample_bytree": "",
        "reg_lambda": "",
        "reg_alpha": "",
        "scale_pos_weight": "",
        "max_delta_step": "",
        "booster": "",
        "tree_method": "",
        "grow_policy": "",
        "eval_metric": "",
        "early_stopping_rounds": "",
        "device": "",
        "cv": "",
        "interaction_constraints": ""
    }
}

|XGB_TYPE |no_of_tickets |last_60_days_tickets |churn_day |churn_day_predictions |
----------------------------------------------------------------------------------
|TRAIN    |5             |2                    |180       |                      |
|TRAIN    |6             |3                    |170       |                      |
|TRAIN    |4             |1                    |190       |                      |
|TRAIN    |5             |1                    |185       |                      |
|TRAIN    |10            |6                    |90        |                      |
<<+10 rows>>
|TEST     |6             |2                    |173       |174.32512             |
|TEST     |13            |6                    |68        |78.522125             |
|TEST     |12            |8                    |69        |70.37183              |
|TEST     |22            |9                    |66        |73.713264             |
|TEST     |32            |9                    |46        |56.3846               |
Total rows: 20

  @LILBro: Yo, here's the lowdown on the model's performance:

{
  "mean_squared_error": 54.14302449585349,
  "r2_score": 0.9820593048686468
}
"#,
                );
                return Ok((csv_builder, false));
            }

            match get_xgb_model_input() {
                Ok((
                    param_column_names,
                    target_column_name,
                    prediction_column_name,
                    model_name_str,
                    xgb_config,
                )) => {
                    let home_dir =
                        env::var("HOME").expect("Unable to determine user home directory");
                    let desktop_path = Path::new(&home_dir).join("Desktop");
                    let csv_db_path = desktop_path.join("csv_db");
                    let model_dir = csv_db_path.join("xgb_models");
                    let model_dir_str = model_dir.to_str().unwrap();

                    let (updated_csv_builder, report_json) = csv_builder
                        .create_xgb_model(
                            &param_column_names,
                            &target_column_name,
                            &prediction_column_name,
                            &model_dir_str,
                            &model_name_str,
                            xgb_config,
                        )
                        .await;

                    // Print the updated table
                    updated_csv_builder.print_table(&big_file_threshold).await;
                    println!();

                    print_insight_level_2("Yo, here's the lowdown on the model's performance:");
                    println!();
                    // Pretty-print the JSON report
                    if let Ok(pretty_report) = to_string_pretty(&report_json) {
                        println!("{}", pretty_report);
                    }
                    println!();
                }
                Err(e) if e.to_string() == "Operation canceled" => {
                    //continue;
                    return Ok((csv_builder, false));
                }

                Err(e) => {
                    println!("Error getting pivot details: {}", e);
                    return Ok((csv_builder, false));
                }
            }
        }

        "3" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Lists out all XGB Models.

|id |model                          |last_modified      |parameters                        |
--------------------------------------------------------------------------------------------
|1  |reg_model.json                 |2024-05-24 05:46:57|no_of_tickets,last_60_days_tickets|
|2  |bin_class_model.json           |2024-05-24 05:46:57|feature1,feature2,feature3        |
|3  |test_model.json                |2024-05-24 06:39:43|no_of_tickets,last_60_days_tickets|
|4  |test_bin_class_model.json      |2024-05-24 08:29:45|feature1,feature2,feature3        |
|5  |test_reg_model.json            |2024-05-24 08:29:46|no_of_tickets,last_60_days_tickets|
|6  |test_binary_classification.json|2024-05-25 13:30:23|feature1,feature2,feature3        |
|7  |test_regression_model.json     |2024-05-26 00:37:23|no_of_tickets,last_60_days_tickets|
Total rows: 7
"#,
                );
                return Ok((csv_builder, false));
            }

            let home_dir = env::var("HOME").expect("Unable to determine user home directory");
            let desktop_path = Path::new(&home_dir).join("Desktop");
            let csv_db_path = desktop_path.join("csv_db");
            let xgb_models_path = csv_db_path.join("xgb_models");
            let xgb_models_path_str = xgb_models_path.to_str().unwrap();

            let mut xgb_models_builder = XgbConnect::get_all_xgb_models(xgb_models_path_str)
                .expect("Failed to load XGB models");

            xgb_models_builder
                .add_column_header("id")
                .order_columns(vec!["id", "..."])
                .cascade_sort(vec![("last_modified".to_string(), "ASC".to_string())])
                .resequence_id_column("id")
                .print_table_all_rows();

            println!();
            return Ok((csv_builder, false));
        }

        "4" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Delete one or more of your XGB Models.

"#,
                );
                return Ok((csv_builder, false));
            }

            let home_dir = env::var("HOME").expect("Unable to determine user home directory");
            let desktop_path = Path::new(&home_dir).join("Desktop");
            let csv_db_path = desktop_path.join("csv_db");
            let _ = delete_xgb_file(&csv_db_path);

            println!();
        }

        "5" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Appends a predictions column leveraging an XGB Model.

"#,
                );
                return Ok((csv_builder, false));
            }

            let home_dir = env::var("HOME").expect("Unable to determine user home directory");
            let desktop_path = Path::new(&home_dir).join("Desktop");
            let csv_db_path = desktop_path.join("csv_db");

            // Call the get_xgb_details function
            match get_xgb_details(&csv_db_path) {
                Ok((path, params)) => {
                    let prediction_column_name =
                        get_user_input_level_2("Name your predictions column: ");

                    if handle_cancel_flag(&prediction_column_name) {
                        return Ok((csv_builder, false));
                    }

                    let path_str = path.to_str().unwrap();

                    //dbg!(&params, &prediction_column_name, &path_str);
                    csv_builder
                        .append_xgb_model_predictions_column(
                            &params,
                            &prediction_column_name,
                            path_str,
                        )
                        .await;

                    csv_builder.print_table(&big_file_threshold).await;
                    println!();
                }
                Err(e) => {
                    eprintln!("An error occurred: {}", e);
                    return Ok((csv_builder, false));
                }
            }
        }

        _ => {
            println!("Invalid option. Please enter a number from 1 to 5.");
            return Ok((csv_builder, false));
        }
    }

    return Ok((csv_builder, true));
}
