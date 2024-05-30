// csv_manager.rs
use crate::csv_appender::handle_append;
use crate::csv_inspector::handle_inspect;
use crate::csv_joiner::handle_join;
use crate::csv_predicter::handle_predict;
use crate::csv_searcher::handle_search;
use crate::csv_tinkerer::handle_tinker;
use crate::csv_transformer::handle_transform;
use crate::user_experience::{
    handle_back_flag, handle_cancel_flag, handle_quit_flag, handle_special_flag,
    handle_special_flag_returning_new_builder, handle_special_flag_without_builder,
};
use crate::user_interaction::{
    determine_action_as_number, determine_action_as_text, determine_action_type_feature_and_flag,
    get_user_input, get_user_input_level_2, print_insight, print_insight_level_2, print_list,
    print_list_level_2,
};
use calamine::{open_workbook, Reader, Xls};
use chrono::{DateTime, Local};
use fuzzywuzzy::fuzz;
use rgwml::csv_utils::CsvBuilder;
use std::collections::HashMap;
use std::fs::{self};
use std::io;
use std::path::PathBuf;
use std::time::SystemTime;

pub fn open_csv_file(csv_db_path: &PathBuf) -> Option<(CsvBuilder, PathBuf)> {
    fn list_csv_files(path: &PathBuf) -> io::Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("csv") {
                files.push(path);
            }
        }
        Ok(files)
    }

    let csv_db_path_str = csv_db_path.to_str().unwrap();

    let mut csv_builder =
        CsvBuilder::get_all_csv_files(csv_db_path_str).expect("Failed to load CSV files");

    csv_builder
        .add_column_header("id")
        .order_columns(vec!["id", "..."])
        .cascade_sort(vec![("last_modified".to_string(), "ASC".to_string())])
        .resequence_id_column("id")
        .print_table_all_rows();
    println!();

    // Extract IDs and corresponding file names from csv_builder
    let binding = Vec::new();
    let data = csv_builder.get_data().unwrap_or(&binding);
    let id_to_file_map: HashMap<usize, &str> = data
        .iter()
        .map(|row| {
            let id = row[0].parse::<usize>().unwrap_or(0);
            let file_name = &row[1];
            (id, file_name.as_str())
        })
        .collect();

    loop {
        match list_csv_files(&csv_db_path) {
            Ok(files) => {
                if files.is_empty() {
                    println!("No files in sight, bro.");
                    return None;
                }

                let choice = get_user_input_level_2("Enter the ID of the file to open: ")
                    .trim()
                    .to_lowercase();

                if handle_back_flag(&choice) || handle_cancel_flag(&choice) {
                    return None;
                }

                if let Ok(index) = choice.parse::<usize>() {
                    if let Some(file_name) = id_to_file_map.get(&index) {
                        let file_path = files.iter().find(|&file| {
                            file.file_name()
                                .and_then(|n| n.to_str())
                                .map_or(false, |n| n == *file_name)
                        });

                        if let Some(file_path) = file_path {
                            if file_path.is_file() {
                                print_insight_level_2(&format!("Opening {}", file_name));
                                return Some((
                                    CsvBuilder::from_csv(file_path.to_str().unwrap()),
                                    file_path.clone(),
                                ));
                            }
                        } else {
                            print_insight_level_2("File not found for the provided ID.");
                        }
                    } else {
                        print_insight_level_2("Invalid ID provided.");
                    }
                } else {
                    print_insight_level_2("Invalid input.");
                }
            }
            Err(_) => {
                print_insight_level_2("Failed to read the directory.");
                return None;
            }
        }
    }
}

pub fn delete_csv_file(csv_db_path: &PathBuf) {
    fn list_csv_files(path: &PathBuf) -> io::Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("csv") {
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

    //let models_path = csv_db_path.join("xgb_models");
    let csv_db_path_str = csv_db_path.to_str().unwrap();

    let mut csv_builder =
        CsvBuilder::get_all_csv_files(csv_db_path_str).expect("Failed to load CSV files");

    //let models_path = csv_db_path.join("xgb_models");
    csv_builder
        .add_column_header("id")
        .order_columns(vec!["id", "..."])
        .cascade_sort(vec![("last_modified".to_string(), "ASC".to_string())])
        .resequence_id_column("id")
        .print_table_all_rows();
    println!();

    // Extract IDs and corresponding file names from xgb_models_builder
    let binding = Vec::new();
    let data = csv_builder.get_data().unwrap_or(&binding);
    let id_to_file_map: HashMap<usize, &str> = data
        .iter()
        .map(|row| {
            let id = row[0].parse::<usize>().unwrap_or(0);
            let file_name = &row[1];
            (id, file_name.as_str())
        })
        .collect();

    loop {
        match list_csv_files(&csv_db_path) {
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
                                    print_insight_level_2(&format!("Failed to delete file: {}", e));
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

                let mut csv_builder_2 = CsvBuilder::get_all_csv_files(csv_db_path_str)
                    .expect("Failed to load CSV files");

                //let models_path = csv_db_path.join("xgb_models");
                csv_builder_2
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

pub fn import(desktop_path: &PathBuf, downloads_path: &PathBuf) -> Option<CsvBuilder> {
    fn system_time_to_date_time(system_time: SystemTime) -> DateTime<Local> {
        let datetime: DateTime<Local> = system_time.into();
        datetime
    }

    fn list_files(path: &PathBuf) -> io::Result<Vec<(PathBuf, SystemTime)>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(extension) = path.extension().and_then(|s| s.to_str()) {
                    if extension == "csv" || extension == "xls" {
                        if let Ok(metadata) = entry.metadata() {
                            if let Ok(modified) = metadata.modified() {
                                files.push((path, modified));
                            }
                        }
                    }
                }
            }
        }
        Ok(files)
    }

    let mut files = list_files(desktop_path).unwrap_or_default();
    files.extend(list_files(downloads_path).unwrap_or_default());

    // Assuming `files` is a Vec<(PathBuf, SystemTime)> or similar
    files.sort_by(|a, b| b.1.cmp(&a.1));

    // Create a vector to hold formatted strings for each file
    let mut file_infos: Vec<String> = Vec::new();

    for (file, modified_date) in files.iter() {
        let formatted_date = system_time_to_date_time(*modified_date)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        if let Some(file_name) = file.file_name().and_then(|n| n.to_str()) {
            // Format each file's information and push it to the vector
            let file_info = format!("{} (Modified: {})", file_name, formatted_date);
            file_infos.push(file_info);
        }
    }

    // Convert Vec<String> to Vec<&str> for `print_list`
    let mut file_info_slices: Vec<&str> = file_infos.iter().map(AsRef::as_ref).collect();
    file_info_slices.push("BACK");
    // Call `print_list` with the vector of file information
    print_list(&file_info_slices);

    let choice = get_user_input("Enter the serial number of the file to open: ");

    let back_option_serial = file_info_slices.len();

    if choice
        .parse::<usize>()
        .ok()
        .map_or(false, |num| num == back_option_serial)
    {
        print_insight("Bailed on that. Heading back to the last menu, bro.");
        return None; // Assuming this is within a function that allows for an early return
    } else {
        // Fuzzy match logic for 'back'
        let options = &["back"];
        let mut highest_score = 0;
        let mut best_match = "";

        for &option in options {
            let score = fuzz::ratio(&choice, option);
            if score > highest_score {
                highest_score = score;
                best_match = option;
            }
        }

        // Check if the best match is 'back' with a score above 60
        if best_match == "back" && highest_score > 60 {
            print_insight("Bailed on that. Heading back to the last menu, bro.");
            return None;
        }
        // Continue with additional logic for handling other inputs or choices
    }

    if let Ok(serial) = choice.parse::<usize>() {
        if serial > 0 && serial <= files.len() {
            let (file_path, _) = &files[serial - 1];
            return if file_path.extension().and_then(|s| s.to_str()) == Some("csv") {
                //dbg!(&file_path);
                //let b = CsvBuilder::from_csv(&file_path.to_str().unwrap());
                //dbg!(&b);
                Some(CsvBuilder::from_csv(file_path.to_str().unwrap()))
            } else {
                // Additional logic for XLS files
                let workbook = open_workbook::<Xls<_>, _>(file_path.to_str().unwrap()).unwrap();
                let sheet_names = workbook.sheet_names();
                if sheet_names.len() > 1 {
                    print_insight("Multiple sheets found. Please select one: ");
                    for (index, name) in sheet_names.iter().enumerate() {
                        print_insight(&format!("{}: {}", index + 1, name));
                    }
                    let sheet_choice = get_user_input("Enter the sheet number: ");
                    if let Ok(sheet_index) = sheet_choice.parse::<usize>() {
                        Some(CsvBuilder::from_xls(
                            file_path.to_str().unwrap(),
                            sheet_index - 1,
                        ))
                    } else {
                        None
                    }
                } else {
                    Some(CsvBuilder::from_xls(file_path.to_str().unwrap(), 1))
                }
            };
        }
    }

    print_insight("Invalid choice or file not accessible.");
    None
}

pub async fn chain_builder(mut builder: CsvBuilder, file_path_option: Option<&str>) {
    //let current_file_path: Option<PathBuf> = file_path_option.map(PathBuf::from);

    fn apply_builder_changes_menu(
        mut csv_builder: CsvBuilder,
        prev_iteration_builder: &CsvBuilder,
        original_csv_builder: &CsvBuilder,
    ) -> Result<(), String> {
        let menu_options = vec![
            "Continue with modified builder",
            "Discard",
            "Load original data from point of import",
        ];
        print_insight_level_2("Apply changes?");
        print_list_level_2(&menu_options);

        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        let selected_option = determine_action_as_number(&menu_options, &choice);

        match selected_option {
            Some(1) => {
                print_insight_level_2("Continuing with modified_builder");
                csv_builder.print_table();
                // Implement the logic for continuing with filtered data
                Ok(())
            }
            Some(2) => {
                print_insight_level_2("Discarding and loading previous state");
                csv_builder
                    .override_with(prev_iteration_builder)
                    .print_table();
                Ok(())
            }
            Some(3) => {
                print_insight_level_2("Loading original data, for you to start from scratch");
                csv_builder
                    .override_with(original_csv_builder)
                    .print_table();
                Ok(())
            }
            _ => Err("Invalid option. Please enter a number from 1 to 2.".to_string()),
        }
    }

    if builder.has_data() {
        let _ = builder.print_table();
        println!();
    }

    let original_csv_builder = CsvBuilder::from_copy(&builder);
    loop {
        let prev_iteration_builder = CsvBuilder::from_copy(&builder);

        //let has_data = builder.has_data();
        print_insight("Choose an action:");

        let menu_options = vec![
            "SEARCH",
            "INSPECT",
            "TINKER",
            "TRANSFORM",
            "APPEND",
            "JOIN",
            "PREDICT",
        ];

        print_list(&menu_options);
        let choice = get_user_input("Enter your choice: ").to_lowercase();

        if handle_special_flag(&choice, &mut builder, file_path_option) {
            continue;
        }

        if handle_special_flag_without_builder(&choice) {
            continue;
        }

        if handle_back_flag(&choice) || &choice == "" {
            break;
        }
        let _ = handle_quit_flag(&choice);

        //dbg!(&choice);
        if let Some(result) = handle_special_flag_returning_new_builder(&choice).await {
            //dbg!(&result);
            match result {
                Ok((_, mut new_builder)) => {
                    // If successful, `new_builder` is the new CsvBuilder instance
                    if new_builder.has_data() && new_builder.has_headers() {
                        print_insight_level_2("Loading new CsvBuilder ...");
                        new_builder.print_table();
                        builder = new_builder;
                        continue;
                    } else {
                        break;
                    }
                }
                Err(e) => {
                    // If there's an error, handle it here
                    println!("An error occurred: {}", e);
                }
            }
        } else {
            //dbg!(&choice);

            let (action_type, action_feature, action_flag) =
                determine_action_type_feature_and_flag(&choice);
            //let selected_option = determine_action_as_text(&menu_options, &choice);
            //let feature_action = "1";
            //let doc_request_flag = "1d";
            dbg!(&action_type, &action_feature, &action_flag);

            match action_type.as_str() {
                "1" => {
                    if let Err(e) = handle_search(&mut builder, file_path_option).await {
                        println!("Error during search: {}", e);
                        continue;
                    }
                }
                "2" => {
                    if let Err(e) = handle_inspect(&mut builder, file_path_option) {
                        println!("Error during inspection: {}", e);
                        continue;
                    }
                }
                "3" => {
                    dbg!(&action_type, &action_feature, &action_flag);
                    let copied_builder = CsvBuilder::from_copy(&builder);
                    let (new_builder, modified) = match handle_tinker(
                        copied_builder,
                        file_path_option,
                        &action_feature,
                        &action_flag,
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(e) => {
                            println!("Error during tinker: {}", e);
                            // Restore the original builder in case of error
                            //            *builder = std::mem::replace(builder, CsvBuilder::new());
                            return;
                        }
                    };

                    // Update the original builder with the new one
                    //builder = new_builder;
                    if modified {
                        println!("The builder has been modified.");
                        match apply_builder_changes_menu(
                            new_builder,
                            &prev_iteration_builder,
                            &original_csv_builder,
                        ) {
                            Ok(_) => (),
                            Err(e) => {
                                println!("{}", e);
                                continue; // Ask for the choice again if there was an error
                            }
                        }
                    } else {
                        println!("The builder has not been modified.");
                    }

                    /*
                    dbg!(&action_type, &action_feature, &action_flag);
                    match handle_tinker(std::mem::replace(builder, CsvBuilder::new()), file_path_option, action_feature, action_flag).await {
                    //match handle_tinker(builder, file_path_option, &action_feature, &action_flag).await {
                        Ok((new_builder, modified)) => {
                            *builder = new_builder; // Update the original builder with the new one
                            if modified {
                                println!("The builder has been modified.");

                                    match apply_builder_changes_menu(
                                        &mut builder,
                                        &prev_iteration_builder,
                                        &original_csv_builder,
                                    ) {
                                        Ok(_) => (),
                                        Err(e) => {
                                            println!("{}", e);
                                            continue; // Ask for the choice again if there was an error
                                        }
                                    }


                            } else {
                                println!("The builder has not been modified.");
                                continue
                            }
                        },
                        Err(e) => {
                            println!("Error during tinker: {}", e);
                            continue;
                        }
                    }
                    */
                }
                "4" => {
                    if let Err(e) = handle_transform(&mut builder, file_path_option).await {
                        println!("Error during transform operation: {}", e);
                        continue;
                    }
                }
                "5" => {
                    if let Err(e) = handle_append(&mut builder, file_path_option).await {
                        println!("Error during pivot operation: {}", e);
                        continue;
                    }
                }
                "6" => {
                    if let Err(e) = handle_join(&mut builder, file_path_option) {
                        println!("Error during join operation: {}", e);
                        continue;
                    }
                }
                "7" => {
                    if let Err(e) = handle_predict(&mut builder, file_path_option).await {
                        println!("Error during predict operation: {}", e);
                        continue;
                    }
                }

                _ => println!("Unknown Action Type: {}", action_type),
            }
        }
    }
}
