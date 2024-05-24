// csv_predicter.rs
use crate::user_experience::{
    handle_back_flag, handle_cancel_flag, handle_quit_flag, handle_special_flag,
};
use crate::user_interaction::{
    determine_action_as_number, get_edited_user_json_input, get_user_input_level_2,
    print_insight_level_2, print_list_level_2,
};
use rgwml::csv_utils::CsvBuilder;
use rgwml::xgb_utils::{XgbConfig, XgbConnect};
use serde_json::Value;
use std::env;
use std::path::Path;

pub async fn handle_predict(
    csv_builder: &mut CsvBuilder,
    file_path_option: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    fn apply_filter_changes_menu(
        csv_builder: &mut CsvBuilder,
        prev_iteration_builder: &CsvBuilder,
        original_csv_builder: &CsvBuilder,
    ) -> Result<(), String> {
        let menu_options = vec![
            "Continue with grouped data",
            "Discard this result, and load previous state",
            "Load original, to start from scratch",
        ];
        print_insight_level_2("Apply changes?");
        print_list_level_2(&menu_options);

        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        let selected_option = determine_action_as_number(&menu_options, &choice);

        match selected_option {
            Some(1) => {
                print_insight_level_2("Continuing with grouped data");
                csv_builder.print_table();
                println!();
                // Implement the logic for continuing with filtered data
                Ok(())
            }
            Some(2) => {
                print_insight_level_2("Discarding this result, and loading previous state");
                csv_builder
                    .override_with(prev_iteration_builder)
                    .print_table();
                println!();
                Ok(())
            }
            Some(3) => {
                print_insight_level_2("Loading original data, for you to start from scratch");
                csv_builder
                    .override_with(original_csv_builder)
                    .print_table();
                println!();
                Ok(())
            }
            _ => Err("Invalid option. Please enter a number from 1 to 3.".to_string()),
        }
    }

    fn get_xgb_model_input(
    ) -> Result<(String, String, String, XgbConfig), Box<dyn std::error::Error>> {
        let xgb_model_input_syntax = r#"{
    "param_columns": "",
    "target_column": "",
    "save_model_as": "",
    "xgb_config": [
        "objective": "",
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
        "interaction_constraints": "",
    ]
}

SYNTAX
======

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
        let save_model_as = parsed_json["save_model_as"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let xgb_config = &parsed_json["xgb_config"];

        let config = XgbConfig {
            objective: xgb_config["objective"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            max_depth: xgb_config["max_depth"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            learning_rate: xgb_config["learning_rate"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            n_estimators: xgb_config["n_estimators"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            gamma: xgb_config["gamma"].as_str().unwrap_or_default().to_string(),
            min_child_weight: xgb_config["min_child_weight"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            subsample: xgb_config["subsample"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            colsample_bytree: xgb_config["colsample_bytree"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            reg_lambda: xgb_config["reg_lambda"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            reg_alpha: xgb_config["reg_alpha"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            scale_pos_weight: xgb_config["scale_pos_weight"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            max_delta_step: xgb_config["max_delta_step"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            booster: xgb_config["booster"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            tree_method: xgb_config["tree_method"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            grow_policy: xgb_config["grow_policy"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            eval_metric: xgb_config["eval_metric"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            early_stopping_rounds: xgb_config["early_stopping_rounds"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            device: xgb_config["device"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            cv: xgb_config["cv"].as_str().unwrap_or_default().to_string(),
            interaction_constraints: xgb_config["interaction_constraints"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
        };

        Ok((param_columns, target_column, save_model_as, config))
    }

    let menu_options = vec![
        "APPEND XGB_TYPE LABEL COLUMN BY RATIO",
        "CREATE XGB MODEL",
        "LIST XGB MODELS",
        "DELETE XGB MODELS",
        "APPEND XGB MODEL PREDICTIONS COLUMN",
    ];

    let original_csv_builder = CsvBuilder::from_copy(csv_builder);

    loop {
        print_insight_level_2("Select an option to group CSV data: ");
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

Appends a XGB_TYPE model column labelling rows as TRAIN, VALIDATE, or TEST, as per the ratio provided.

"#,
                    );
                    continue;
                }

                let xgb_ratio_str =
                    get_user_input_level_2("Enter the TRAIN:VALIDATE:TEST ratio to label data by (for instance: 70:20:10): ");

                if handle_cancel_flag(&xgb_ratio_str) {
                    continue;
                }

                csv_builder.append_xgb_label_by_ratio_column(&xgb_ratio_str);

                if csv_builder.has_data() {
                    csv_builder.print_table();
                    println!();
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

            Some(2) => {
                if choice.to_lowercase() == "2d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Creates an XGB Model.

"#,
                    );
                    continue;
                }

                match get_xgb_model_input() {
                    Ok((param_column_names, target_column_name, model_name_str, xgb_config)) => {
                        let home_dir =
                            env::var("HOME").expect("Unable to determine user home directory");
                        let desktop_path = Path::new(&home_dir).join("Desktop");
                        let csv_db_path = desktop_path.join("csv_db");
                        let model_dir = csv_db_path.join("xgb_models");
                        let model_dir_str = model_dir.to_str().unwrap();

                        csv_builder
                            .create_xgb_model(
                                &param_column_names,
                                &target_column_name,
                                &model_dir_str,
                                &model_name_str,
                                xgb_config,
                            )
                            .await
                            .print_table();
                        println!();

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

                    Err(e) => println!("Error getting pivot details: {}", e),
                }
            }

            Some(3) => {
                if choice.to_lowercase() == "3d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Lists out all XGB Models.

"#,
                    );
                    continue;
                }

                let home_dir = env::var("HOME").expect("Unable to determine user home directory");
                let desktop_path = Path::new(&home_dir).join("Desktop");
                let csv_db_path = desktop_path.join("csv_db");
                let csv_db_path_str = csv_db_path.to_str().unwrap();

                *csv_builder = XgbConnect::get_all_xgb_models(csv_db_path_str)
                    .expect("Failed to load XGB models");

                csv_builder.print_table();
                println!();

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

            _ => {
                println!("Invalid option. Please enter a number from 1 to 5.");
                continue;
            }
        }

        //println!();
    }

    Ok(())
}
