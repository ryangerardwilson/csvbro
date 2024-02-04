// csv_manager.rs
use crate::csv_appender::handle_append;
use crate::csv_inspector::handle_inspect;
use crate::settings::{manage_config_file, DbPreset};
use crate::user_interaction::{
    get_edited_user_json_input, get_edited_user_sql_input, get_user_input, get_user_input_level_2,
    get_user_sql_input, print_insight, print_insight_level_2, print_list,
};

use calamine::{open_workbook, Reader, Xls};
use chrono::{DateTime, Local};
use fuzzywuzzy::fuzz;
use rgwml::csv_utils::{CalibConfig, CsvBuilder};
use serde_json::json;
use std::env;
use std::error::Error;
use std::fs::{self};
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;
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

    match list_csv_files(&csv_db_path) {
        Ok(mut files) => {
            if files.is_empty() {
                print_insight("No files in sight, bro.");
                return None;
            }

            files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

            for (index, file) in files.iter().enumerate() {
                if let Some(file_name) = file.file_name().and_then(|n| n.to_str()) {
                    print_list(&format!("{}: {}", index + 1, file_name));
                }
            }

            let choice = get_user_input("Punch in the serial number or a slice of the file name to LOAD, or hit 'back' to bail.\nWhat's it gonna be?: ")
                .to_lowercase();

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

            match choice.parse::<usize>() {
                Ok(serial) if serial > 0 && serial <= files.len() => {
                    let file_path = files[serial - 1].clone();
                    if file_path.is_file() {
                        if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
                            print_insight(&format!("Opening {}", file_name));
                        }
                        return Some((
                            CsvBuilder::from_csv(file_path.to_str().unwrap()),
                            file_path,
                        ));
                    }
                }
                _ => (),
            }

            // Fuzzy search and opening logic
            let best_match_result = files
                .iter()
                .filter_map(|path| {
                    path.file_name()
                        .and_then(|n| n.to_str())
                        .map(|name| (path.clone(), fuzz::ratio(&choice, name)))
                })
                .max_by_key(|&(_, score)| score);

            if let Some((best_match, _)) = best_match_result {
                if best_match.is_file() {
                    if let Some(file_name) = best_match.file_name().and_then(|n| n.to_str()) {
                        print_insight(&format!("Opening {}", file_name));
                    }
                    return Some((
                        CsvBuilder::from_csv(best_match.to_str().unwrap()),
                        best_match.clone(),
                    ));
                }
            }

            print_insight("No matching file found.");
        }
        Err(_) => {
            print_insight("Failed to read the directory.");
        }
    }
    None
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

    match list_csv_files(csv_db_path) {
        Ok(mut files) => {
            if files.is_empty() {
                print_insight("No files in sight, bro.");
                return;
            }

            files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

            for (index, file) in files.iter().enumerate() {
                if let Some(file_name) = file.file_name().and_then(|n| n.to_str()) {
                    print_list(&format!("{}: {}", index + 1, file_name));
                }
            }

            let choice = get_user_input("Punch in the serial number or a slice of the file name to DELETE, or hit 'back' to bail.\nWhat's it gonna be?: ")
                .to_lowercase();

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
                return;
            }

            let mut file_deleted = false;

            match choice.parse::<usize>() {
                Ok(serial) if serial > 0 && serial <= files.len() => {
                    let file_path = &files[serial - 1];
                    if file_path.is_file() {
                        if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
                            print_insight_level_2(&format!("Deleting {}", file_name));
                            if let Err(e) = fs::remove_file(file_path) {
                                print_insight(&format!("Failed to delete file: {}", e));
                            } else {
                                print_insight("File deleted successfully.");
                                file_deleted = true;
                            }
                        }
                    }
                }
                _ => (),
            }

            // Proceed to fuzzy search only if no file was deleted by index
            if !file_deleted {
                let best_match_result = files
                    .iter()
                    .filter_map(|path| {
                        path.file_name()
                            .and_then(|n| n.to_str())
                            .map(|name| (path, fuzz::ratio(&choice, name)))
                    })
                    .max_by_key(|&(_, score)| score);

                if let Some((best_match, _)) = best_match_result {
                    if best_match.is_file() {
                        if let Some(file_name) = best_match.file_name().and_then(|n| n.to_str()) {
                            print_insight_level_2(&format!("Deleting {}", file_name));
                            if let Err(e) = fs::remove_file(best_match) {
                                print_insight(&format!("Failed to delete file: {}", e));
                            } else {
                                print_insight("File deleted successfully.");
                            }
                        }
                    }
                } else {
                    if !file_deleted {
                        print_insight("No matching file found for deletion.");
                    }
                }
            }
        }
        Err(_) => {
            print_insight("Failed to read the directory.");
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

    // Sort files by modified date in descending order
    files.sort_by(|a, b| b.1.cmp(&a.1));

    for (index, (file, modified_date)) in files.iter().enumerate() {
        let formatted_date = system_time_to_date_time(*modified_date)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        if let Some(file_name) = file.file_name().and_then(|n| n.to_str()) {
            print_list(&format!(
                "{}: {} (Modified: {})",
                index + 1,
                file_name,
                formatted_date
            ));
        }
    }

    let choice = get_user_input("Enter the serial number of the file to open: ");

    if let Ok(serial) = choice.parse::<usize>() {
        if serial > 0 && serial <= files.len() {
            let (file_path, _) = &files[serial - 1];
            return if file_path.extension().and_then(|s| s.to_str()) == Some("csv") {
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

enum DbType {
    MsSql,
    MySql,
}

#[allow(unused_assignments)]
pub async fn query() -> Result<CsvBuilder, Box<dyn std::error::Error>> {
    fn get_db_type() -> Result<(DbType, Option<DbPreset>), Box<dyn std::error::Error>> {
        fn process_option(
            index: usize,
            presets: &[DbPreset],
            db_choice_index: usize,
        ) -> Result<(DbType, Option<DbPreset>), Box<dyn Error>> {
            match index {
                i if i < db_choice_index => {
                    let preset = &presets[i];
                    let db_type = match preset.db_type.to_lowercase().as_str() {
                        "mssql" => DbType::MsSql,
                        "mysql" => DbType::MySql,
                        _ => return Err("Unknown database type in preset".into()),
                    };
                    Ok((db_type, Some(preset.clone())))
                }
                i if i == db_choice_index => Ok((DbType::MsSql, None)),
                i if i == db_choice_index + 1 => Ok((DbType::MySql, None)),
                _ => Err("return_to_main".into()), // This is for the "back" option
            }
        }

        let mut presets = Vec::new(); // Declare a variable to store presets

        let _ = manage_config_file(|config| {
            presets = config.db_presets.clone(); // Assign the presets here
            Ok(()) // Return Ok(()) as expected by the function signature
        });
        let mut options = presets
            .iter()
            .map(|p| p.name.to_lowercase())
            .collect::<Vec<_>>();
        let db_choice_index = presets.len();
        options.push("mssql".to_string());
        options.push("mysql".to_string());
        options.push("back".to_string());

        print_insight_level_2("Choose a database:");
        for (index, option) in options.iter().enumerate() {
            let formatted_message = format!("{}: {}", index + 1, option);
            print_list(&formatted_message);
        }

        let input = get_user_input_level_2("Enter your choice: ").to_lowercase();

        // Check if input directly matches an index
        if let Ok(index) = input.parse::<usize>() {
            if index > 0 && index <= options.len() {
                return process_option(index - 1, &presets, db_choice_index);
            }
        }

        // Fuzzy match logic if no direct index match
        let (best_match_index, best_match_score) = options
            .iter()
            .enumerate()
            .map(|(index, option)| (index, fuzz::ratio(&input, option)))
            .max_by_key(|&(_, score)| score)
            .unwrap_or((0, 0));

        if best_match_score < 60 {
            return Err("No matching option found".into());
        }

        process_option(best_match_index, &presets, db_choice_index)
    }

    fn get_confirmation_input() -> Result<String, Box<dyn std::error::Error>> {
        let input = get_user_input_level_2(
            "What next? (retry/show all rows/inspect/append/save as/back): ",
        )
        .to_lowercase();
        let options = &["retry", "show all rows", "inspect", "append", "save as", "back"];
        let mut highest_score = 0;
        let mut best_match = "";

        for &option in options {
            let score = fuzz::ratio(&input, option);
            if score > highest_score {
                highest_score = score;
                best_match = option;
            }
        }

        match best_match {
            "yes" => Ok("yes".to_string()),
            "retry" => Ok("retry".to_string()),
            "show all rows" => Ok("show all rows".to_string()),
            "back" => Ok("back".to_string()),
            "inspect" => Ok("inspect".to_string()),
            "append" => Ok("append".to_string()),
            "save as" => Ok("save as".to_string()),
            _ => Err("Invalid option".into()),
        }
    }

    fn get_query_failed_confirmation_input() -> Result<String, Box<dyn std::error::Error>> {
        let input = get_user_input_level_2("Do you want to retry? (retry/back): ").to_lowercase();
        let options = &["retry", "back"];
        let mut highest_score = 0;
        let mut best_match = "";

        for &option in options {
            let score = fuzz::ratio(&input, option);
            if score > highest_score {
                highest_score = score;
                best_match = option;
            }
        }

        match best_match {
            "retry" => Ok("retry".to_string()),
            "back" => Ok("back".to_string()),
            _ => Err("Invalid option".into()),
        }
    }

    let (mut db_type, mut preset_option) = match get_db_type() {
        Ok(db) => db,
        Err(e) => {
            if e.to_string() == "return_to_main" {
                return Err("User chose to go back".into());
            } else {
                return Err(e);
            }
        }
    };

    let mut csv_builder: CsvBuilder;
    let mut last_sql_query = String::new();
    let mut confirmation = String::new();

    // Use preset details if available, otherwise prompt for details
    let (mut username, mut password, mut host, mut database) = if let Some(preset) = preset_option {
        (
            preset.username,
            preset.password,
            preset.host,
            preset.database,
        )
    } else {
        (String::new(), String::new(), String::new(), String::new())
    };

    loop {
        let _query_result: Result<CsvBuilder, Box<dyn std::error::Error>>;

        match db_type {
            DbType::MsSql => {
                // Existing connection logic for i2e1
                if username.is_empty()
                    || password.is_empty()
                    || host.is_empty()
                    || database.is_empty()
                {
                    username = get_user_input_level_2("Enter MSSQL username: ");
                    password = get_user_input_level_2("Enter MSSQL password: ");
                    host = get_user_input_level_2("Enter MSSQL server: ");
                    database = get_user_input_level_2("Enter MSSQL database name: ");
                }

                //dbg!(&confirmation, &last_sql_query);
                let sql_query = if confirmation == "retry" && !last_sql_query.is_empty() {
                    // Use vim_edit only if confirmation is "retry"
                    let new_query = get_edited_user_sql_input(last_sql_query.clone());
                    last_sql_query = new_query.clone();
                    new_query
                } else if confirmation != "inspect"
                    && confirmation != "append"
                    && confirmation != "show all rows"
                    && confirmation != "save as"
                {
                    // Get new query from user, except when confirmation is "inspect"
                    let new_query = get_user_sql_input();
                    last_sql_query = new_query.clone();
                    new_query
                } else {
                    // If confirmation is "inspect", use the last SQL query
                    last_sql_query.clone()
                };

                let back_score = fuzz::ratio(&sql_query.to_lowercase(), "back");
                if back_score > 65 {
                    let result = get_db_type(); // Return to database selection
                    if let Ok((new_db_type, new_preset)) = result {
                        db_type = new_db_type;
                        preset_option = new_preset;
                        continue; // Return to database selection
                    }
                }

                let start_time = Instant::now();
                let query_execution_result = CsvBuilder::from_mssql_query(
                    &username, &password, &host, &database, &sql_query,
                )
                .await;
                let elapsed_time = start_time.elapsed();

                if let Err(e) = query_execution_result {
                    println!("Failed to execute query: {}", e);

                    match get_query_failed_confirmation_input() {
                        Ok(response) => {
                            if response == "retry" {
                                confirmation = "retry".to_string(); // Set confirmation to retry for the next loop iteration
                                continue; // User chose to retry
                            } else {
                                println!("Returning to the main menu.");
                                return Err("Query execution failed".into());
                            }
                        }
                        Err(_) => {
                            println!("Invalid option. Please try again.");
                            continue; // Ask for confirmation again
                        }
                    }
                } else {
                    csv_builder = query_execution_result.unwrap();
                    csv_builder.print_table(); // Print the table on success
                    println!("Executiom Time: {:?}", elapsed_time);

                    confirmation = String::new(); // Reset confirmation for the next loop iteration
                }
            }

            DbType::MySql => {
                // Existing connection logic for i2e1

                if username.is_empty()
                    || password.is_empty()
                    || host.is_empty()
                    || database.is_empty()
                {
                    username = get_user_input_level_2("Enter MYSQL username: ");
                    password = get_user_input_level_2("Enter MYSQL password: ");
                    host = get_user_input_level_2("Enter MYSQL server: ");
                    database = get_user_input_level_2("Enter MYSQL database name: ");
                }

                //dbg!(&confirmation, &last_sql_query);
                let sql_query = if confirmation == "retry" && !last_sql_query.is_empty() {
                    // Use vim_edit only if confirmation is "retry"
                    let new_query = get_edited_user_sql_input(last_sql_query.clone());
                    last_sql_query = new_query.clone();
                    new_query
                } else if confirmation != "inspect"
                    && confirmation != "append"
                    && confirmation != "show all rows"
                    && confirmation != "save as"
                {
                    // Get new query from user, except when confirmation is "inspect"
                    let new_query = get_user_sql_input();
                    last_sql_query = new_query.clone();
                    new_query
                } else {
                    // If confirmation is "inspect", use the last SQL query
                    last_sql_query.clone()
                };

                let back_score = fuzz::ratio(&sql_query.to_lowercase(), "back");
                if back_score > 65 {
                    let result = get_db_type();
                    if let Ok((new_db_type, new_preset)) = result {
                        db_type = new_db_type;
                        preset_option = new_preset;
                        continue; // Return to database selection
                    }
                }

                let start_time = Instant::now();
                let query_execution_result = CsvBuilder::from_mysql_query(
                    &username, &password, &host, &database, &sql_query,
                )
                .await;
                let elapsed_time = start_time.elapsed();

                if let Err(e) = query_execution_result {
                    println!("Failed to execute query: {}", e);

                    match get_query_failed_confirmation_input() {
                        Ok(response) => {
                            if response == "retry" {
                                confirmation = "retry".to_string(); // Set confirmation to retry for the next loop iteration
                                continue; // User chose to retry
                            } else {
                                println!("Returning to the main menu.");
                                return Err("Query execution failed".into());
                            }
                        }
                        Err(_) => {
                            println!("Invalid option. Please try again.");
                            continue; // Ask for confirmation again
                        }
                    }
                } else {
                    csv_builder = query_execution_result.unwrap();
                    csv_builder.print_table(); // Print the table on success
                    println!("Executiom Time: {:?}", elapsed_time);
                    confirmation = String::new(); // Reset confirmation for the next loop iteration
                }
            } // DbType::MySql and other cases
              // ...
        };

        // Ask user for confirmation
        println!();

        match get_confirmation_input() {
            Ok(response) => confirmation = response, // Removed 'let', updates existing variable
            Err(_) => {
                println!("Invalid option. Please enter 'yes', 'retry', or 'back'.");
                continue; // Ask for confirmation again
            }
        };

        match confirmation.as_str() {
            "yes" => break,      // Confirms and exits the loop
            "retry" => continue, // Repeats the query for the same database type
            "show all rows" => {
                if csv_builder.has_data() {
                    csv_builder.print_table_all_rows();
                    println!();
                }
            }
            "inspect" => {
                if let Err(e) = handle_inspect(&mut csv_builder) {
                    println!("Error during inspection: {}", e);
                    continue; // Handle error or let the user try again
                }
            }
            "append" => {
                //dbg!(&csv_builder);
                if let Err(e) = handle_append(&mut csv_builder) {
                    println!("Error during append operation: {}", e);
                    continue; // Handle error or let the user try again
                }
                //dbg!(&csv_builder);
            }
            "save as" => {
                //dbg!(&csv_builder);

                let home_dir = env::var("HOME").expect("Unable to determine user home directory");
                let desktop_path = Path::new(&home_dir).join("Desktop");
                let csv_db_path = desktop_path.join("csv_db");

                let file_name =
                    get_user_input_level_2("Enter file name to save (without extension): ");
                let full_file_name = if file_name.ends_with(".csv") {
                    file_name
                } else {
                    format!("{}.csv", file_name)
                };
                let file_path = csv_db_path.join(full_file_name);
                let _ = csv_builder.save_as(file_path.to_str().unwrap());
                print_insight_level_2(&format!("CSV file saved at {}", file_path.display()));

                //dbg!(&csv_builder);
            }
            "back" => {
                // Get the database type and preset again for a new query, handle "back" option
                let result = get_db_type();
                match result {
                    Ok((new_db_type, new_preset)) => {
                        db_type = new_db_type;
                        preset_option = new_preset;

                        // Reset the preset details if a new preset is selected
                        if let Some(preset) = &preset_option {
                            username = preset.username.clone();
                            password = preset.password.clone();
                            host = preset.host.clone(); // Assuming 'host' is equivalent to 'server'
                            database = preset.database.clone();
                        } else {
                            // Clear the details if no preset is selected
                            username.clear();
                            password.clear();
                            host.clear();
                            database.clear();
                        }
                    }
                    Err(e) => {
                        if e.to_string() == "return_to_main" {
                            return Err("User chose to go back".into());
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
            _ => unreachable!(), // This case should not occur as get_confirmation_input handles it
        }
    }

    // dbg!(&csv_builder.headers, &csv_builder.data); // Optionally, debug print the CsvBuilder
    Ok(csv_builder)
}

pub fn chain_builder(mut builder: CsvBuilder, file_path_option: Option<&str>) {
    fn determine_action(input: &str, has_data: bool, has_headers: bool) -> &'static str {
        let mut actions = Vec::new();

        // Add "update headers" or "set headers" based on has_headers
        if has_headers {
            actions.push("update_headers");
        } else {
            actions.push("set_headers");
        }

        // Add "add row" if has_data is true
        if has_data {
            actions.push("show_all_rows");
            actions.push("calibrate");
            actions.push("add_rows");
            actions.push("update_row");
            actions.push("inspect");
            actions.push("append");
            actions.push("delete_rows");
            actions.push("sort");
        }

        // Always add "save" and "back"
        actions.push("save");
        actions.push("save_as");
        actions.push("back");

        let mut highest_score = 0;
        let mut best_match = "";

        for &action in &actions {
            let score = fuzz::ratio(input, action);
            if score > highest_score {
                highest_score = score;
                best_match = action;
            }
        }

        best_match
    }

    let current_file_path: Option<PathBuf> = file_path_option.map(PathBuf::from);

    if builder.has_data() {
        let _ = builder.print_table();
        println!();
    }

    let home_dir = env::var("HOME").expect("Unable to determine user home directory");
    let desktop_path = Path::new(&home_dir).join("Desktop");
    let csv_db_path = desktop_path.join("csv_db");

    loop {
        let has_data = builder.has_data();
        let has_headers = builder.has_headers(); // Assuming this method exists

        let action_prompt = if has_data {
            if has_headers {
                "Choose action >> show_all_rows/calibrate/update_headers/add_rows/update_row/inspect/append/delete_rows/sort/save/save_as/back: "
            } else {
                "Choose action >> show_all_rows/calibrate/set_headers/add_rows/update_row/inspect/append/delete_rows/sort/save/save_as/back: "
            }
        } else {
            if has_headers {
                "Choose action >> update_headers/back: "
            } else {
                "Choose action >> set_headers/back: "
            }
        };

        let action = get_user_input(action_prompt);

        match determine_action(&action, has_data, has_headers) {
            "show_all_rows" => {
                if builder.has_data() {
                    builder.print_table_all_rows();
                    println!();
                }
            }

            "calibrate" => {
                println!();

                // Define the JSON syntax for calibration settings
                let calib_syntax = r#"{
    "header_is_at_row": "",
    "rows_range_from": ["", ""]
}

SYNTAX
======

### Example 1

{
    "header_is_at_row": "3",
    "rows_range_from": ["5", "*"]
}

### Example 2

{
    "header_is_at_row": "5",
    "rows_range_from": ["7", "50"]
}


        "#;

                // Get user input
                let calib_json = get_edited_user_json_input(calib_syntax.to_string());
                //dbg!(&calib_json);

                // Parse the user input
                let calib_config = {
                    let parsed_calib_config =
                        match serde_json::from_str::<serde_json::Value>(&calib_json) {
                            Ok(config) => config,
                            Err(e) => {
                                eprintln!("Error parsing JSON: {}", e);
                                return; // Exit the function early
                            }
                        };

                    // Extract calibration settings
                    let header_row = Box::leak(
                        parsed_calib_config["header_is_at_row"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string()
                            .into_boxed_str(),
                    );
                    let start_range = Box::leak(
                        parsed_calib_config["rows_range_from"][0]
                            .as_str()
                            .unwrap_or_default()
                            .to_string()
                            .into_boxed_str(),
                    );
                    let end_range = Box::leak(
                        parsed_calib_config["rows_range_from"][1]
                            .as_str()
                            .unwrap_or_default()
                            .to_string()
                            .into_boxed_str(),
                    );

                    CalibConfig {
                        header_is_at_row: header_row,
                        rows_range_from: (start_range, end_range),
                    }
                };

                // Apply the calibration
                builder.calibrate(calib_config);

                if builder.has_data() {
                    builder.print_table();
                    println!();
                }
            }

            "set_headers" => {
                println!();

                let headers_json = json!({
                    "headers": Vec::<String>::new()
                });

                let headers_json_str = match serde_json::to_string_pretty(&headers_json) {
                    Ok(json) => json,
                    Err(e) => {
                        eprintln!("Error creating JSON string: {}", e);
                        return;
                    }
                };

                let edited_json = get_edited_user_sql_input(headers_json_str);

                let edited_headers: serde_json::Value = match serde_json::from_str(&edited_json) {
                    Ok(headers) => headers,
                    Err(e) => {
                        eprintln!("Error parsing JSON string: {}", e);
                        return;
                    }
                };

                let headers = match edited_headers["headers"].as_array() {
                    Some(array) => array
                        .iter()
                        .map(|val| {
                            val.as_str()
                                .unwrap_or_default()
                                .to_lowercase()
                                .replace(" ", "_")
                        })
                        .collect::<Vec<String>>(),
                    None => {
                        eprintln!("Invalid format for new headers");
                        return;
                    }
                };

                let header_slices: Vec<&str> = headers.iter().map(AsRef::as_ref).collect();
                builder.set_header(header_slices);

                if builder.has_data() {
                    builder.print_table();
                    println!();
                }
            }
            "update_headers" => {
                println!();

                let existing_headers = builder.get_headers().unwrap_or(&[]).to_vec();

                let headers_json = json!({
                    "existing_headers": existing_headers,
                    "new_headers": Vec::<String>::new()
                });

                let headers_json_str = match serde_json::to_string_pretty(&headers_json) {
                    Ok(json) => json,
                    Err(e) => {
                        eprintln!("Error creating JSON string: {}", e);
                        return;
                    }
                };

                let edited_json = get_edited_user_sql_input(headers_json_str);

                let edited_headers: serde_json::Value = match serde_json::from_str(&edited_json) {
                    Ok(headers) => headers,
                    Err(e) => {
                        eprintln!("Error parsing JSON string: {}", e);
                        return;
                    }
                };

                let new_headers = match edited_headers["new_headers"].as_array() {
                    Some(array) => array
                        .iter()
                        .map(|val| {
                            val.as_str()
                                .unwrap_or_default()
                                .to_lowercase()
                                .replace(" ", "_")
                        })
                        .collect::<Vec<String>>(),
                    None => {
                        eprintln!("Invalid format for new headers");
                        return;
                    }
                };

                // Ensure new headers list is the same length as existing headers
                let max_length = builder.get_headers().map_or(0, |headers| headers.len());
                let mut updated_headers = new_headers;
                updated_headers.resize(max_length, String::new());

                let header_slices: Vec<&str> = updated_headers.iter().map(AsRef::as_ref).collect();
                builder.set_header(header_slices);

                if builder.has_data() {
                    builder.print_table();
                    println!();
                }
            }

            "add_rows" if has_data => {
                println!();

                if let Some(headers) = builder.get_headers() {
                    // Start defining the JSON array syntax
                    let mut json_array_str = "[\n  {\n".to_string();

                    // Loop through headers and append them as keys in the JSON array string, excluding auto-computed columns
                    for (i, header) in headers.iter().enumerate() {
                        if header != "id" && header != "c@" && header != "u@" {
                            json_array_str.push_str(&format!("    \"{}\": \"\"", header));
                            if i < headers.len() - 1 {
                                json_array_str.push_str(",\n");
                            }
                        }
                    }

                    // Close the first JSON object and start the syntax explanation
                    json_array_str.push_str("\n  }\n]");

                    let syntax_explanation = r#"

SYNTAX
======

### Example

[
    {
        "column1": "value1",
        "column2": "value2",
        // ...
    },
    {
        "column1": "value1",
        "column2": "value2",
        // ...
    }
    // ...
]

        "#;

                    // Combine the dynamic JSON syntax with the syntax explanation
                    let full_syntax = json_array_str + syntax_explanation;

                    // Get user input
                    let rows_json_str = get_edited_user_json_input(full_syntax);

                    // Parse the user input
                    let rows_json: Vec<serde_json::Value> =
                        match serde_json::from_str(&rows_json_str) {
                            Ok(json) => json,
                            Err(e) => {
                                eprintln!("Error parsing JSON string: {}", e);
                                return; // Exit the function early if there's an error
                            }
                        };

                    let mut all_rows = Vec::new();

                    // Logic to find the current maximum ID
                    let mut next_id = builder
                        .get_data()
                        .iter()
                        .filter_map(|row| {
                            row.get(headers.iter().position(|h| h == "id").unwrap_or(0))
                        })
                        .filter_map(|id_str| id_str.parse::<usize>().ok())
                        .max()
                        .unwrap_or(0)
                        + 1; // Start from the next available ID

                    for row_json in rows_json {
                        let mut row_data_owned = Vec::new();

                        for header in headers {
                            if header == "id" {
                                row_data_owned.push(next_id.to_string()); // Use the next available ID
                                next_id += 1; // Increment for the next row
                            } else if header == "c@" {
                                // Handle c@ column
                            } else if header == "u@" {
                                // Handle u@ column
                            } else {
                                let cell_value = match &row_json[header] {
                                    serde_json::Value::String(s) => s.to_string(),
                                    serde_json::Value::Array(arr) => {
                                        serde_json::to_string(arr).unwrap_or_default()
                                    }
                                    serde_json::Value::Object(obj) => {
                                        serde_json::to_string(obj).unwrap_or_default()
                                    }
                                    // Add more cases as needed
                                    _ => row_json[header].as_str().unwrap_or_default().to_string(),
                                };
                                row_data_owned.push(cell_value);
                            }
                        }

                        all_rows.push(row_data_owned);
                    }

                    // Convert each Vec<String> to Vec<&str> before passing to add_rows
                    let rows_as_str_slices = all_rows
                        .iter()
                        .map(|row| row.iter().map(AsRef::as_ref).collect::<Vec<&str>>())
                        .collect::<Vec<Vec<&str>>>();

                    builder.add_rows(rows_as_str_slices);
                    builder.print_table();
                    println!();
                    continue;
                } else {
                    print_insight("No headers set. Cannot add rows.");
                }
            }

            "update_row" => {
                println!();

                if !builder.has_data() {
                    eprintln!("No data available to update.");
                    return;
                }

                // Display existing data
                builder.print_table();
                println!();

                let use_id_for_update = builder
                    .get_headers()
                    .map_or(false, |headers| headers.contains(&"id".to_string()));
                let zero_based_index: usize;
                let mut original_id = String::new();

                if use_id_for_update {
                    let id_str = get_user_input("Enter the id of the row to update: ");
                    let id = id_str.trim();

                    if let Some((index, _)) = builder
                        .get_data()
                        .iter()
                        .enumerate()
                        .find(|(_, row)| row.get(0) == Some(&id.to_string()))
                    {
                        zero_based_index = index;
                        original_id = id.to_string();
                    } else {
                        eprintln!("ID not found.");
                        return;
                    }
                } else {
                    let row_index_str = get_user_input("Enter the index of the row to update: ");
                    let row_index: usize = match row_index_str.trim().parse() {
                        Ok(num) => num,
                        Err(_) => {
                            eprintln!("Invalid input for row index.");
                            return;
                        }
                    };

                    zero_based_index = row_index.saturating_sub(1);
                }

                if zero_based_index >= builder.get_data().len() {
                    eprintln!("Row index out of range.");
                    return;
                }

                if let Some(existing_row) = builder.get_data().get(zero_based_index) {
                    if let Some(headers) = builder.get_headers() {
                        let mut json_str = "{\n".to_string();

                        for (i, header) in headers.iter().enumerate() {
                            // Skip the 'id' field in the JSON string
                            if header == "id" {
                                continue;
                            }

                            let default_value = "".to_string();
                            let value = existing_row.get(i).unwrap_or(&default_value);
                            json_str.push_str(&format!("  \"{}\": \"{}\"", header, value));
                            if i < headers.len() - 1 {
                                json_str.push_str(",\n");
                            }
                        }

                        json_str.push_str("\n}");

                        let row_json_str = json_str;

                        let edited_json = get_edited_user_sql_input(row_json_str);

                        let edited_row: serde_json::Value = match serde_json::from_str(&edited_json)
                        {
                            Ok(row) => row,
                            Err(e) => {
                                eprintln!("Error parsing JSON string: {}", e);
                                return;
                            }
                        };

                        let new_row = headers
                            .iter()
                            .map(|header| {
                                if header == "id" && use_id_for_update {
                                    original_id.clone()
                                } else {
                                    match &edited_row[header] {
                                        serde_json::Value::String(s) => s.to_string(),
                                        serde_json::Value::Array(arr) => {
                                            serde_json::to_string(arr).unwrap_or_default()
                                        }
                                        serde_json::Value::Object(obj) => {
                                            serde_json::to_string(obj).unwrap_or_default()
                                        }
                                        // Add more cases for other types as needed
                                        _ => edited_row[header]
                                            .as_str()
                                            .unwrap_or_default()
                                            .to_string(),
                                    }
                                }
                            })
                            .collect::<Vec<String>>();

                        builder.update_row_by_row_number(
                            if use_id_for_update {
                                zero_based_index + 1
                            } else {
                                zero_based_index
                            },
                            new_row.iter().map(AsRef::as_ref).collect(),
                        );
                    } else {
                        eprintln!("No headers set. Cannot update row.");
                    }
                } else {
                    eprintln!("Row index out of range.");
                }

                builder.print_table();
                println!();
            }

            "inspect" => {
                if let Err(e) = handle_inspect(&mut builder) {
                    println!("Error during inspection: {}", e);
                    continue;
                }
            }
            "append" => {
                if let Err(e) = handle_append(&mut builder) {
                    println!("Error during append operation: {}", e);
                    continue;
                }
            }

            "delete_rows" => {
                println!();

                if !builder.has_data() {
                    eprintln!("No data available for deletion.");
                    return;
                }

                // Display existing data
                builder.print_table();
                println!();

                let use_id_for_deletion = builder
                    .get_headers()
                    .map_or(false, |headers| headers.contains(&"id".to_string()));

                let row_identifiers_str = get_user_input_level_2("Enter the identifiers (ID or indices) of the rows to delete (comma-separated), or type 'back' to return: ");

                let back_keywords = ["back", "b", "ba", "bck"];

                if back_keywords
                    .iter()
                    .any(|&kw| row_identifiers_str.trim().eq_ignore_ascii_case(kw))
                {
                    continue;
                }

                let mut deleted_count = 0;

                if use_id_for_deletion {
                    // Parse as IDs
                    let ids: Vec<&str> = row_identifiers_str.split(',').map(|s| s.trim()).collect();

                    for id in ids {
                        if builder.delete_row_by_id(id) {
                            deleted_count += 1;
                        } else {
                            eprintln!("No row found with ID '{}'.", id);
                        }
                    }
                } else {
                    // Parse as row indices
                    let row_indices: Vec<usize> = row_identifiers_str
                        .split(',')
                        .filter_map(|s| s.trim().parse::<usize>().ok())
                        .collect();

                    if row_indices.is_empty() {
                        eprintln!("No valid indices provided.");
                        return;
                    }

                    // Sort indices in descending order to avoid index shift during deletion
                    let mut sorted_indices = row_indices;
                    sorted_indices.sort_by(|a, b| b.cmp(a));

                    for index in sorted_indices {
                        if builder.delete_row_by_row_number(index) {
                            deleted_count += 1;
                        } else {
                            eprintln!("Row index {} out of range.", index);
                        }
                    }
                }

                if deleted_count > 0 {
                    println!("{} row(s) deleted successfully.", deleted_count);
                }

                // Print updated table
                builder.print_table();
                println!();
            }

            "sort" => {
                println!();

                // Define the JSON syntax for sort settings
                let sort_syntax = r#"{
    "sort_orders": [
        {"column": "", "order": ""}
    ]
}

SYNTAX
======

### Example

{
    "sort_orders": [
        {"column": "Name", "order": "ASC"},
        {"column": "Age", "order": "DESC"}
    ]
}

"#;

                // Get user input
                let sort_json = get_edited_user_json_input(sort_syntax.to_string());
                //dbg!(&sort_json);

                // Parse the user input
                let sort_orders = {
                    let parsed_sort_orders =
                        match serde_json::from_str::<serde_json::Value>(&sort_json) {
                            Ok(config) => config,
                            Err(e) => {
                                eprintln!("Error parsing JSON: {}", e);
                                return; // Exit the function early
                            }
                        };

                    // Extract sort orders
                    parsed_sort_orders["sort_orders"]
                        .as_array()
                        .unwrap_or(&vec![])
                        .iter()
                        .filter_map(|order| {
                            let column = order["column"].as_str()?.to_owned();
                            let order = order["order"].as_str()?.to_owned();
                            Some((column, order))
                        })
                        .collect::<Vec<(String, String)>>()
                };

                // Convert to references for cascade_sort
                let sort_orders_refs = sort_orders
                    .iter()
                    .map(|(col, ord)| (col.as_str(), ord.as_str()))
                    .collect::<Vec<(&str, &str)>>();

                // Apply the cascade sort
                builder.cascade_sort(sort_orders_refs);

                if builder.has_data() {
                    builder.print_table();
                    println!();
                }
            }

            "save" if has_data => {
                if let Some(ref path) = current_file_path {
                    // Save to the existing file path
                    let _ = builder.save_as(path.to_str().unwrap());
                    print_insight(&format!("CSV file saved at {}", path.display()));
                } else {
                    let file_name =
                        get_user_input_level_2("Enter file name to save (without extension): ");
                    let full_file_name = if file_name.ends_with(".csv") {
                        file_name
                    } else {
                        format!("{}.csv", file_name)
                    };
                    let file_path = csv_db_path.join(full_file_name);
                    let _ = builder.save_as(file_path.to_str().unwrap());
                    print_insight(&format!("CSV file saved at {}", file_path.display()));
                }
            }

            "save_as" if has_data => {
                let file_name =
                    get_user_input_level_2("Enter file name to save (without extension): ");
                let full_file_name = if file_name.ends_with(".csv") {
                    file_name
                } else {
                    format!("{}.csv", file_name)
                };
                let file_path = csv_db_path.join(full_file_name);
                let _ = builder.save_as(file_path.to_str().unwrap());
                print_insight(&format!("CSV file saved at {}", file_path.display()));
                //break; // Exit the loop after saving
            }
            "back" => {
                break;
            }
            //"done" => break,
            _ => print_insight("Unrecognized action, please try again."),
        }
    }
}
