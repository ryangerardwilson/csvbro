// csv_grouper.rs
use crate::user_experience::{
    handle_back_flag, handle_cancel_flag, handle_quit_flag, handle_special_flag,
};
use crate::user_interaction::{
    determine_action_as_number, get_user_input_level_2, print_insight_level_2, print_list_level_2,
};
use rgwml::csv_utils::CsvBuilder;
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

  @LILbro: Enter the column name to group the data by: item
  @LILbro: Enter the name of the grouped column: logs

|item      |logs                                         |logs_count |
----------------------------------------------------------------------
|milk shake|[{"calorie_count":"300","id":"2","item":"milk|1          |
|pizza     |[{"calorie_count":"500","id":"1","item":"pizz|2          |
|potatoe   |[{"calorie_count":"100","id":"3","item":"pota|1          |
Total rows: 3
"#,
                    );
                    continue;
                }

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
