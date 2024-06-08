// csv_manager.rs
use crate::config::Config;
use crate::csv_appender::handle_append;
use crate::csv_inspector::handle_inspect;
use crate::csv_joiner::handle_join;
use crate::csv_predicter::handle_predict;
use crate::csv_searcher::handle_search;
use crate::csv_tinkerer::handle_tinker;
use crate::csv_transformer::handle_transform;
use crate::user_experience::{
    handle_back_flag, handle_cancel_flag, handle_query_retry_flag, handle_quit_flag,
    handle_special_flag, handle_special_flag_without_builder,
};
use crate::user_interaction::{
    determine_action_as_number, determine_action_type_feature_and_flag, get_user_input,
    get_user_input_level_2, print_insight, print_insight_level_2, print_list,
};
use chrono::{DateTime, Local};
use fuzzywuzzy::fuzz;
use rgwml::csv_utils::CsvBuilder;
use rgwml::dc_utils::DataContainer;
use serde_json::from_str;
use std::collections::HashMap;
use std::env;
use std::fs::read_to_string;
use std::fs::{self};
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

pub async fn open_csv_file(csv_db_path: &PathBuf) -> Option<(CsvBuilder, PathBuf)> {
    fn list_data_files(path: &PathBuf) -> io::Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(extension) = path.extension().and_then(|s| s.to_str()) {
                    if ["csv", "xls", "xlsx", "h5"].contains(&extension) {
                        files.push(path);
                    }
                }
            }
        }
        Ok(files)
    }

    let csv_db_path_str = csv_db_path.to_str().unwrap();

    let mut csv_builder =
        DataContainer::get_all_data_files(csv_db_path_str).expect("Failed to load Data files");

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
        match list_data_files(&csv_db_path) {
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
                                let file_ext = file_path.extension().and_then(|s| s.to_str());

                                match file_ext {
                                    Some("csv") => {
                                        return Some((
                                            CsvBuilder::from_csv(file_path.to_str().unwrap()),
                                            file_path.clone(),
                                        ));
                                    }
                                    Some("xls") | Some("xlsx") => {
                                        let sheet_names = if file_ext == Some("xls") {
                                            DataContainer::get_xls_sheet_names(
                                                file_path.to_str().unwrap(),
                                            )
                                        } else {
                                            DataContainer::get_xlsx_sheet_names(
                                                file_path.to_str().unwrap(),
                                            )
                                        }
                                        .expect("Failed to get sheet names");

                                        let sheet_names_slices: Vec<&str> =
                                            sheet_names.iter().map(String::as_str).collect();
                                        println!();
                                        print_insight("Available Sheets:");
                                        print_list(&sheet_names_slices);

                                        /*
                                        println!("Available sheets:");
                                        for (i, sheet) in sheet_names.iter().enumerate() {
                                            println!("{}: {}", i + 1, sheet);
                                        }
                                        */

                                        let sheet_choice = get_user_input_level_2(
                                            "Enter the sheet name or index to open: ",
                                        )
                                        .trim()
                                        .to_string();

                                        let csv_builder =
                                            if sheet_choice.chars().all(char::is_numeric) {
                                                //let sheet_index = sheet_choice.parse::<usize>().unwrap();
                                                if file_ext == Some("xls") {
                                                    CsvBuilder::from_xls(
                                                        file_path.to_str().unwrap(),
                                                        &sheet_choice,
                                                        "SHEET_ID",
                                                    )
                                                } else {
                                                    CsvBuilder::from_xlsx(
                                                        file_path.to_str().unwrap(),
                                                        &sheet_choice,
                                                        "SHEET_ID",
                                                    )
                                                }
                                            } else {
                                                if file_ext == Some("xls") {
                                                    CsvBuilder::from_xls(
                                                        file_path.to_str().unwrap(),
                                                        &sheet_choice,
                                                        "SHEET_NAME",
                                                    )
                                                } else {
                                                    CsvBuilder::from_xlsx(
                                                        file_path.to_str().unwrap(),
                                                        &sheet_choice,
                                                        "SHEET_NAME",
                                                    )
                                                }
                                            };

                                        return Some((csv_builder, file_path.clone()));
                                    }
                                    Some("h5") => {
                                        let dataset_names = DataContainer::get_h5_dataset_names(
                                            file_path.to_str().unwrap(),
                                        )
                                        .expect("Failed to get dataset names");

                                        /*
                                        println!("Available datasets:");
                                        for (i, dataset) in dataset_names.iter().enumerate() {
                                            println!("{}: {}", i + 1, dataset);
                                        }
                                        */
                                        /*
                                        let dataset_names_slices: Vec<&str> =
                                            dataset_names.iter().map(String::as_str).collect();
                                        println!();
                                        print_insight("Available Datasets:");
                                        print_list(&dataset_names_slices);

                                        let dataset_choice = get_user_input_level_2(
                                            "Enter the dataset name or index to open: ",
                                        )
                                        .trim()
                                        .to_string();

                                        let csv_builder =
                                            if dataset_choice.chars().all(char::is_numeric) {
                                                CsvBuilder::from_h5(
                                                    file_path.to_str().unwrap(),
                                                    &dataset_choice,
                                                    "DATASET_ID",
                                                )
                                            } else {
                                                CsvBuilder::from_h5(
                                                    file_path.to_str().unwrap(),
                                                    &dataset_choice,
                                                    "DATASET_NAME",
                                                )
                                            };
                                        */
                                        let dataset_names_slices: Vec<&str> =
                                            dataset_names.iter().map(String::as_str).collect();
                                        println!();
                                        print_insight("Available Datasets:");
                                        print_list(&dataset_names_slices);

                                        let dataset_choice = get_user_input_level_2(
                                            "Enter the dataset ID to open (starting from 1): ",
                                        )
                                        .trim()
                                        .to_string();

                                        let csv_builder =
                                            if let Ok(id) = dataset_choice.parse::<usize>() {
                                                if id > 0 && id <= dataset_names.len() {
                                                    let adjusted_id = id - 1;

                                                    CsvBuilder::from_h5(
                                                        file_path.to_str().unwrap(),
                                                        &dataset_names[adjusted_id],
                                                        "DATASET_NAME",
                                                    )
                                                    .await
                                                } else {
                                                    panic!("Invalid dataset ID");
                                                }
                                            } else {
                                                panic!("Please enter a valid numeric ID");
                                            };

                                        return Some((csv_builder, file_path.clone()));
                                    }
                                    _ => {
                                        print_insight_level_2("Unsupported file type.");
                                    }
                                }
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
    fn list_data_files(path: &PathBuf) -> io::Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(extension) = path.extension().and_then(|s| s.to_str()) {
                    if ["csv", "xls", "xlsx", "h5"].contains(&extension) {
                        files.push(path);
                    }
                }
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

    let csv_db_path_str = csv_db_path.to_str().unwrap();

    let mut csv_builder =
        DataContainer::get_all_data_files(csv_db_path_str).expect("Failed to load data files");

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
        match list_data_files(&csv_db_path) {
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

                let mut csv_builder_2 = DataContainer::get_all_data_files(csv_db_path_str)
                    .expect("Failed to load data files");

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

pub async fn import(desktop_path: &PathBuf, downloads_path: &PathBuf) -> Option<CsvBuilder> {
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
                    if ["csv", "xls", "xlsx", "h5"].contains(&extension) {
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

    // Sort files by modified date
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
        return None;
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
    }

    if let Ok(serial) = choice.parse::<usize>() {
        if serial > 0 && serial <= files.len() {
            let (file_path, _) = &files[serial - 1];
            let file_extension = file_path.extension().and_then(|s| s.to_str());

            return match file_extension {
                Some("csv") => Some(CsvBuilder::from_csv(file_path.to_str().unwrap())),
                Some("xls") | Some("xlsx") => {
                    let sheet_names = if file_extension == Some("xls") {
                        DataContainer::get_xls_sheet_names(file_path.to_str().unwrap())
                    } else {
                        DataContainer::get_xlsx_sheet_names(file_path.to_str().unwrap())
                    }
                    .expect("Failed to get sheet names");

                    println!("Available sheets:");
                    for (i, sheet) in sheet_names.iter().enumerate() {
                        println!("{}: {}", i + 1, sheet);
                    }

                    let sheet_choice = get_user_input("Enter the sheet name or index to open: ")
                        .trim()
                        .to_string();

                    if sheet_choice.chars().all(char::is_numeric) {
                        if file_extension == Some("xls") {
                            Some(CsvBuilder::from_xls(
                                file_path.to_str().unwrap(),
                                &sheet_choice,
                                "SHEET_ID",
                            ))
                        } else {
                            Some(CsvBuilder::from_xlsx(
                                file_path.to_str().unwrap(),
                                &sheet_choice,
                                "SHEET_ID",
                            ))
                        }
                    } else {
                        if file_extension == Some("xls") {
                            Some(CsvBuilder::from_xls(
                                file_path.to_str().unwrap(),
                                &sheet_choice,
                                "SHEET_NAME",
                            ))
                        } else {
                            Some(CsvBuilder::from_xlsx(
                                file_path.to_str().unwrap(),
                                &sheet_choice,
                                "SHEET_NAME",
                            ))
                        }
                    }
                }
                Some("h5") => {
                    let dataset_names =
                        DataContainer::get_h5_dataset_names(file_path.to_str().unwrap())
                            .expect("Failed to get dataset names");

                    /*
                    println!("Available datasets:");
                    for (i, dataset) in dataset_names.iter().enumerate() {
                        println!("{}: {}", i + 1, dataset);
                    }

                    let dataset_choice =
                        get_user_input("Enter the dataset name or index to open: ")
                            .trim()
                            .to_string();

                    if dataset_choice.chars().all(char::is_numeric) {
                        Some(CsvBuilder::from_h5(
                            file_path.to_str().unwrap(),
                            &dataset_choice,
                            "DATASET_ID",
                        ))
                    } else {
                        Some(CsvBuilder::from_h5(
                            file_path.to_str().unwrap(),
                            &dataset_choice,
                            "DATASET_NAME",
                        ))
                    }
                    */
                    let dataset_names_slices: Vec<&str> =
                        dataset_names.iter().map(String::as_str).collect();
                    println!();
                    print_insight("Available Datasets:");
                    print_list(&dataset_names_slices);

                    let dataset_choice =
                        get_user_input_level_2("Enter the dataset ID to open (starting from 1): ")
                            .trim()
                            .to_string();

                    let csv_builder = if let Ok(id) = dataset_choice.parse::<usize>() {
                        if id > 0 && id <= dataset_names.len() {
                            let adjusted_id = id - 1;
                            CsvBuilder::from_h5(
                                file_path.to_str().unwrap(),
                                &dataset_names[adjusted_id],
                                "DATASET_NAME",
                            )
                            .await
                        } else {
                            panic!("Invalid dataset ID");
                        }
                    } else {
                        panic!("Please enter a valid numeric ID");
                    };

                    Some(csv_builder)
                }
                _ => {
                    print_insight("Unsupported file type.");
                    None
                }
            };
        }
    }

    print_insight("Invalid choice or file not accessible.");
    None
}

pub async fn import_from_url() -> Option<CsvBuilder> {
    let url = get_user_input("Enter URL to import data from: ");

    if !url.starts_with("https://docs.google.com/spreadsheets") {
        print_insight("Hey there, we only support Google Sheets URLs for now. Cool?");
        return None;
    }

    let builder = CsvBuilder::from_publicly_viewable_google_sheet(&url).await;

    Some(builder)
}

pub async fn chain_builder(mut builder: CsvBuilder, file_path_option: Option<&str>) {
    let home_dir = match env::var("HOME") {
        Ok(dir) => dir,
        Err(e) => {
            eprintln!("Unable to determine user home directory: {}", e);
            return;
        }
    };
    let desktop_path = Path::new(&home_dir).join("Desktop");
    let csv_db_path = desktop_path.join("csv_db");
    let config_path = PathBuf::from(csv_db_path).join("bro.config");

    let file_contents = match read_to_string(&config_path) {
        Ok(contents) => contents,
        Err(e) => {
            eprintln!("Failed to read config file: {}", e);
            return;
        }
    };

    let valid_json_part = match file_contents.split("SYNTAX").next() {
        Some(part) => part,
        None => {
            eprintln!("Invalid configuration format");
            return;
        }
    };

    let config: Config = match from_str(valid_json_part) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Failed to parse configuration: {}", e);
            return;
        }
    };

    let big_file_threshold = &config.big_file_threshold_in_megabytes;

    //let current_file_path: Option<PathBuf> = file_path_option.map(PathBuf::from);

    if builder.has_data() {
        let _ = builder.print_table(&big_file_threshold).await;
        println!();
    }

    print_insight("Choose an action:");

    let action_menu_options = vec![
        "SEARCH",
        "INSPECT",
        "TINKER",
        "TRANSFORM",
        "APPEND",
        "JOIN",
        "PREDICT",
    ];

    print_list(&action_menu_options);

    let original_csv_builder = CsvBuilder::from_copy(&builder);
    loop {
        let prev_iteration_builder = CsvBuilder::from_copy(&builder);

        let choice = get_user_input("Enter your choice: ").to_lowercase();

        if handle_back_flag(&choice) || &choice == "" {
            break;
        }

        if handle_special_flag(&choice, &mut builder, file_path_option) {
            continue;
        }

        if handle_special_flag_without_builder(&choice) {
            continue;
        }

        let _ = handle_quit_flag(&choice);

        let (action_type, action_feature, action_flag) =
            determine_action_type_feature_and_flag(&choice);

        match action_type.as_str() {
            "1" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_search(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during search: {}", e);
                        return;
                    }
                };

                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);

                    println!("The builder has been modified.");
                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }
            "2" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (_new_builder, _modified) = match handle_inspect(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during tinker: {}", e);
                        return;
                    }
                };
                continue;
            }
            "3" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_tinker(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during tinker: {}", e);
                        return;
                    }
                };

                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);
                    println!("The builder has been modified.");
                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }
            "4" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_transform(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during search: {}", e);
                        return;
                    }
                };
                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);
                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }
            "5" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_append(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during search: {}", e);
                        return;
                    }
                };
                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);
                    //.print_table().await;

                    println!("The builder has been modified.");
                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }
            "6" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_join(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during search: {}", e);
                        return;
                    }
                };

                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);

                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }
            "7" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_predict(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during search: {}", e);
                        return;
                    }
                };
                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);

                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }

            _ => print_insight("That's not on the menu, man!"),
        }
    }
}

pub async fn query_chain_builder(
    mut builder: CsvBuilder,
    mut choice: String,
    file_path_option: Option<&str>,
) -> bool {
    let home_dir = match env::var("HOME") {
        Ok(dir) => dir,
        Err(e) => {
            eprintln!("Unable to determine user home directory: {}", e);
            return false;
        }
    };
    let desktop_path = Path::new(&home_dir).join("Desktop");
    let csv_db_path = desktop_path.join("csv_db");
    let config_path = PathBuf::from(csv_db_path).join("bro.config");

    let file_contents = match read_to_string(&config_path) {
        Ok(contents) => contents,
        Err(e) => {
            eprintln!("Failed to read config file: {}", e);
            return false;
        }
    };

    let valid_json_part = match file_contents.split("SYNTAX").next() {
        Some(part) => part,
        None => {
            eprintln!("Invalid configuration format");
            return false;
        }
    };

    let config: Config = match from_str(valid_json_part) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Failed to parse configuration: {}", e);
            return false;
        }
    };

    let big_file_threshold = &config.big_file_threshold_in_megabytes;

    let action_menu_options = vec![
        "SEARCH",
        "INSPECT",
        "TINKER",
        "TRANSFORM",
        "APPEND",
        "JOIN",
        "PREDICT",
    ];

    let original_csv_builder = CsvBuilder::from_copy(&builder);
    let mut is_first_iteration = true;
    let mut retry_invoked = false;
    loop {
        let prev_iteration_builder = CsvBuilder::from_copy(&builder);

        if !is_first_iteration {
            choice = get_user_input("Enter your choice: ").to_lowercase();
        }

        if handle_back_flag(&choice) {
            break;
        }
        let _ = handle_quit_flag(&choice);

        if handle_query_retry_flag(&choice) {
            //break;
            retry_invoked = true;
            break;
        }

        let (action_type, action_feature, action_flag) =
            determine_action_type_feature_and_flag(&choice);

        match action_type.as_str() {
            "1" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_search(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during search: {}", e);
                        return retry_invoked;
                    }
                };

                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);
                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }
            "2" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (_new_builder, _modified) = match handle_inspect(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during tinker: {}", e);
                        return retry_invoked;
                    }
                };
            }
            "3" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_tinker(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during tinker: {}", e);
                        return retry_invoked;
                    }
                };

                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);
                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }
            "4" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_transform(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during search: {}", e);
                        return retry_invoked;
                    }
                };
                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);
                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }
            "5" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_append(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during search: {}", e);
                        return retry_invoked;
                    }
                };
                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);
                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }
            "6" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_join(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during search: {}", e);
                        return retry_invoked;
                    }
                };

                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);
                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }
            "7" => {
                let copied_builder = CsvBuilder::from_copy(&builder);
                let (new_builder, modified) = match handle_predict(
                    copied_builder,
                    file_path_option,
                    &action_type,
                    &action_feature,
                    &action_flag,
                    action_menu_options.clone(),
                    &big_file_threshold,
                )
                .await
                {
                    Ok(result) => result,
                    Err(e) => {
                        println!("Error during search: {}", e);
                        return retry_invoked;
                    }
                };
                // Update the original builder with the new one
                if modified {
                    builder.override_with(&new_builder);
                    match apply_builder_changes_menu(
                        &mut builder,
                        &prev_iteration_builder,
                        &original_csv_builder,
                        &big_file_threshold,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            println!("{}", e);
                            continue; // Ask for the choice again if there was an error
                        }
                    }
                }
            }

            _ => print_insight("That's not on the menu, man!"),
        }
        is_first_iteration = false;
    }
    retry_invoked
}

pub async fn apply_builder_changes_menu(
    csv_builder: &mut CsvBuilder,
    prev_iteration_builder: &CsvBuilder,
    original_csv_builder: &CsvBuilder,
    big_file_threshold: &str,
) -> Result<(), String> {
    let menu_options = vec![
        "Continue with modified builder",
        "Discard",
        "Load original data from point of import",
    ];
    print_insight_level_2("Apply changes?");
    print_list(&menu_options);

    let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
    let selected_option = determine_action_as_number(&menu_options, &choice);

    match selected_option {
        Some(1) => {
            print_insight_level_2("Continuing with modified_builder");
            csv_builder.print_table(&big_file_threshold).await;
            // Implement the logic for continuing with filtered data
            Ok(())
        }
        Some(2) => {
            print_insight_level_2("Discarding and loading previous state");
            csv_builder
                .override_with(prev_iteration_builder)
                .print_table(&big_file_threshold)
                .await;
            Ok(())
        }
        Some(3) => {
            print_insight_level_2("Loading original data, for you to start from scratch");
            csv_builder
                .override_with(original_csv_builder)
                .print_table(&big_file_threshold)
                .await;
            Ok(())
        }
        _ => Err("Invalid option. Please enter a number from 1 to 2.".to_string()),
    }
}
