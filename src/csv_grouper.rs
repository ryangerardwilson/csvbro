// csv_grouper.rs
use crate::user_experience::{
    handle_back_flag, handle_cancel_flag, handle_quit_flag, handle_special_flag,
};
use crate::user_interaction::{
    determine_action_as_number, get_edited_user_json_input, get_user_input_level_2,
    print_insight_level_2, print_list_level_2,
};
use rgwml::csv_utils::CsvBuilder;
use serde_json::Value;
use std::fs;

pub async fn handle_group(
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

    let menu_options = vec![
        "TRANSFORM INTO GROUPED INDEX",
        "SPLIT INTO GROUPED CSV FILES",
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

Groups a log table by a particular column, transmuting all data pertaining to unique values of that column into a ordered array stored in a grouped column.

|id |item      |calorie_count |
-------------------------------
|1  |pizza     |500           |
|2  |milk shake|300           |
|3  |potatoe   |100           |
|4  |pizza     |600           |
Total rows: 4

  @LILbro: Executing this JSON query:
{
  "group_by_column": "item",
  "grouped_column_name": "history",
  "feature_flags": {
    "id": "",
    "item": "",
    "calorie_count": "NUMERICAL_MAX"
  }
}

|item      |history                                      |history_count |history_unique_count |calorie_count_numerical_max |
----------------------------------------------------------------------------------------------------------------------------
|milk shake|[{"calorie_count":"300","id":"2","item":"milk|1             |1                    |300                         |
|pizza     |[{"calorie_count":"500","id":"1","item":"pizz|2             |2                    |600                         |
|potatoe   |[{"calorie_count":"100","id":"3","item":"pota|1             |1                    |100                         |
Total rows: 3

# Available Feature Flahs

The following feature flags can be used to perform different types of calculations on the specified columns:
 - `COUNT_UNIQUE` - Counts the unique values in the column.
 - `NUMERICAL_MAX` - Finds the maximum numerical value in the column.
 - `NUMERICAL_MIN` - Finds the minimum numerical value in the column.
 - `NUMERICAL_SUM` - Calculates the sum of numerical values in the column.
 - `NUMERICAL_MEAN` - Calculates the mean (average) of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_MEDIAN` - Calculates the median of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_STANDARD_DEVIATION` - Calculates the standard deviation of numerical values in the column, rounded to two decimal places.
 - `DATETIME_MAX` - Finds the maximum datetime value in the column, based on specified formats.
 - `DATETIME_MIN` - Finds the minimum datetime value in the column, based on specified formats.
 - `MODE` - Finds the most frequent value in the column.
 - `BOOL_PERCENT` - Calculates the percentage of `1`s in the column, assuming the values are either `1` or `0`, rounded to two decimal places.
"#,
                    );
                    continue;
                }

                if let Some(headers) = csv_builder.get_headers() {
                    let mut json_str = "{\n".to_string();

                    json_str.push_str(&format!("  \"group_by_column\": \"\",\n"));
                    json_str.push_str(&format!("  \"grouped_column_name\": \"\",\n"));

                    json_str.push_str("  \"feature_flags\": {\n");

                    for (i, header) in headers.iter().enumerate() {
                        json_str.push_str(&format!("    \"{}\": \"\"", header));
                        if i < headers.len() - 1 {
                            json_str.push_str(",\n");
                        }
                    }

                    json_str.push_str("\n  }\n");

                    json_str.push_str("}");

                    let syntax = r#"

SYNTAX
======

{
  "group_by_column": "item",          // The column to GROUP BY with
  "grouped_column_name": "history",   // The name of the column compressing all row data
  "feature_flags": {
    "id": "",
    "item": "",                       // Leave blank if you don't want it include it
    "calorie_count": "NUMERICAL_MAX"  // Specify a feature flag
  }
}

# Available Feature Flags

The following feature flags can be used to perform different types of calculations on the specified columns:
 - `COUNT_UNIQUE` - Counts the unique values in the column.
 - `NUMERICAL_MAX` - Finds the maximum numerical value in the column.
 - `NUMERICAL_MIN` - Finds the minimum numerical value in the column.
 - `NUMERICAL_SUM` - Calculates the sum of numerical values in the column.
 - `NUMERICAL_MEAN` - Calculates the mean (average) of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_MEDIAN` - Calculates the median of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_STANDARD_DEVIATION` - Calculates the standard deviation of numerical values in the column, rounded to two decimal places.
 - `DATETIME_MAX` - Finds the maximum datetime value in the column, based on specified formats.
 - `DATETIME_MIN` - Finds the minimum datetime value in the column, based on specified formats.
 - `MODE` - Finds the most frequent value in the column.
 - `BOOL_PERCENT` - Calculates the percentage of `1`s in the column, assuming the values are either `1` or `0`, rounded to two decimal places.
                "#;

                    json_str.push_str(syntax);

                    let row_json_str = json_str;

                    let edited_json = get_edited_user_json_input(row_json_str);

                    let edited_value: Value =
                        serde_json::from_str(&edited_json).expect("Invalid JSON format");

                    // Extract values from the edited JSON
                    let group_by_column = edited_value["group_by_column"]
                        .as_str()
                        .expect("group_by_column is not a string");
                    let grouped_column_name = edited_value["grouped_column_name"]
                        .as_str()
                        .expect("grouped_column_name is not a string");

                    let mut feature_flags = Vec::new();
                    if let Value::Object(map) = &edited_value["feature_flags"] {
                        for (key, value) in map.iter() {
                            if let Value::String(feature_flag) = value {
                                if !feature_flag.is_empty() {
                                    feature_flags.push((key.clone(), feature_flag.clone()));
                                }
                            }
                        }
                    }

                    csv_builder.grouped_index_transform(
                        &group_by_column,
                        &grouped_column_name,
                        feature_flags,
                    );
                }

                /*
                                let group_by_column_name_str =
                                    get_user_input_level_2("Enter the column name to group the data by: ");

                                if handle_cancel_flag(&group_by_column_name_str) {
                                    continue;
                                }

                                let grouped_column_name_str =
                                    get_user_input_level_2("Enter the name of the grouped column: ");

                                if handle_cancel_flag(&grouped_column_name_str) {
                                    continue;
                                }

                                csv_builder
                                    .grouped_index_transform(&group_by_column_name_str, &grouped_column_name_str);

                */

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

Generates csv files at a specified directory, splitting your current files based on a GROUP BY column.

|id |item      |calorie_count |
-------------------------------
|1  |pizza     |500           |
|2  |milk shake|300           |
|3  |potatoe   |100           |
|4  |pizza     |600           |
Total rows: 4

  @LILbro: Enter the column name to group the data by: item
  @LILbro: Enter file path of directory to store grouped data: /home/rgw/Desktop/split_csvs_dr
  @LILBro: Split completed at /home/rgw/Desktop/split_csvs_dr. 3 files generated!

/// In the concerned directory:

  rgw@rgw-asus:~/Desktop/split_csvs_dr$ ls
   'group_split_by_milk shake_in_item.csv'   
   group_split_by_potatoe_in_item.csv
   group_split_by_pizza_in_item.csv
"#,
                    );
                    continue;
                }

                let group_by_column_name_str =
                    get_user_input_level_2("Enter the column name to group the data by: ");

                if handle_cancel_flag(&group_by_column_name_str) {
                    continue;
                }

                let grouped_data_dir_path_str =
                    get_user_input_level_2("Enter file path of directory to store grouped data: ");

                if handle_cancel_flag(&grouped_data_dir_path_str) {
                    continue;
                }

                let _ = csv_builder.split_as(&group_by_column_name_str, &grouped_data_dir_path_str);

                //let insight = format!("Split completed at {}", grouped_data_dir_path_str);
                //print_insight_level_2(&insight);
                let paths = fs::read_dir(grouped_data_dir_path_str.clone()).unwrap();
                let file_count = paths.count();
                let insight = format!(
                    "Split completed at {}. {} files generated!",
                    grouped_data_dir_path_str, file_count
                );
                print_insight_level_2(&insight);
                println!();

                continue;
                /*
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
                */
            }

            _ => {
                println!("Invalid option. Please enter a number from 1 to 2.");
                continue;
            }
        }

        //println!();
    }

    Ok(())
}
