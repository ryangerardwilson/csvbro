// csv_manager.rs
use crate::csv_inspector::handle_inspect;
use crate::csv_joiner::handle_join;
use crate::csv_pivoter::handle_pivot;
use crate::csv_searcher::handle_search;
use crate::csv_tinkerer::handle_tinker;
use crate::settings::{manage_db_config_file, DbPreset};
use crate::user_experience::{
    handle_back_flag, handle_query_retry_flag, handle_query_special_flag, handle_quit_flag,
    handle_special_flag,
};
use crate::user_interaction::{
    determine_action_as_text,
    //determine_action_as_text_or_number
    get_edited_user_sql_input,
    get_user_input,
    get_user_input_level_2,
    get_user_sql_input,
    print_insight,
    print_insight_level_2,
    print_list,
};
use calamine::{open_workbook, Reader, Xls};
use chrono::{DateTime, Local};
use fuzzywuzzy::fuzz;
use regex::Regex;
use rgwml::csv_utils::CsvBuilder;
use std::error::Error;
use std::fs::{self};
use std::io;
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

            // Collect file names into a Vec<&str>
            let file_names: Vec<String> = files
                .iter()
                .filter_map(|file| file.file_name()?.to_str().map(String::from))
                .collect();

            // Since print_list expects a Vec<&str>, convert Vec<String> to Vec<&str>
            let mut file_name_slices: Vec<&str> = file_names.iter().map(AsRef::as_ref).collect();
            file_name_slices.push("BACK");
            // Now, call print_list with this vector
            print_list(&file_name_slices);

            let choice = get_user_input("What's it gonna be?: ").to_lowercase();

            // Assuming 'back' is always the last option
            let back_option_number = file_name_slices.len();

            // Check if the user's choice is a number and if it matches the serial number for 'back'
            if choice.parse::<usize>().ok() == Some(back_option_number) {
                print_insight("Bailed on that. Heading back to the last menu, bro.");
                return None; // Assuming this is within a function that can return None for some control flow
            } else {
                // Handle other choices or input errors
            }

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

            // Collect file names into a Vec<&str>
            let file_names: Vec<String> = files
                .iter()
                .filter_map(|file| file.file_name()?.to_str().map(String::from))
                .collect();

            let mut file_name_slices: Vec<&str> = file_names.iter().map(AsRef::as_ref).collect();
            file_name_slices.push("BACK");

            // Now, call print_list with this vector
            print_list(&file_name_slices);

            let choice = get_user_input("Punch in the serial number or a slice of the file name to DELETE, or hit 'back' to bail.\nWhat's it gonna be?: ")
    .trim().to_lowercase();

            // Assuming 'back' is always the last option
            let back_option_serial = file_name_slices.len();

            // Check if the user's choice is a number and matches the serial number for 'back'
            if choice
                .parse::<usize>()
                .ok()
                .map_or(false, |num| num == back_option_serial)
            {
                print_insight("Bailed on that. Heading back to the last menu, bro.");
                return; // Assuming this is within a function that allows for an early return
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
                    return;
                }
                // Continue with additional logic for handling other inputs or choices
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

enum DbType {
    MsSql,
    MySql,
}

#[allow(unused_assignments)]
//pub async fn query() {
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

        let _ = manage_db_config_file(|config| {
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
        let options_slices: Vec<&str> = options.iter().map(AsRef::as_ref).collect();

        print_insight_level_2("Choose a database:");
        print_list(&options_slices);

        let input = get_user_input_level_2("Enter your choice: ").to_lowercase();

        // Direct Index Selection
        if let Ok(index) = input.parse::<usize>() {
            if index > 0 && index <= options.len() {
                return process_option(index - 1, &presets, db_choice_index);
            }
        }

        // Starts With Match
        if let Some(index) = options.iter().position(|option| option.starts_with(&input)) {
            return process_option(index, &presets, db_choice_index);
        }

        // Existing Fuzzy Match Logic
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

    let (db_type, preset_option) = match get_db_type() {
        Ok(db) => db,
        Err(e) => {
            if e.to_string() == "return_to_main" {
                return Err("User chose to go back".into());
            } else {
                return Err(e);
            }
        }
    };

    //let mut csv_builder: CsvBuilder;
    let mut csv_builder: CsvBuilder = CsvBuilder::new();
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

                if confirmation == "TINKER"
                    || confirmation == "SEARCH"
                    || confirmation == "INSPECT"
                    || confirmation == "PIVOT"
                    || confirmation == "JOIN"
                {
                    csv_builder.print_table();
                    confirmation = String::new();
                } else {
                    let sql_query = if confirmation == "@r" && !last_sql_query.is_empty() {
                        // Use vim_edit only if confirmation is "retry"
                        let new_query = get_edited_user_sql_input(last_sql_query.clone());
                        last_sql_query = new_query.clone();
                        new_query
                    } else {
                        // Get new query from user, except when confirmation is "inspect"
                        let new_query = get_user_sql_input();
                        last_sql_query = new_query.clone();
                        new_query
                    };

                    /*
                                        let start_time = Instant::now();
                                        let query_execution_result = CsvBuilder::from_mssql_query(
                                            &username, &password, &host, &database, &sql_query,
                                        )
                                        .await;
                                        let elapsed_time = start_time.elapsed();
                    */

                    let start_time = Instant::now();
                    //let query_execution_result;

                    let query_execution_result: Result<CsvBuilder, Box<dyn std::error::Error>>;

                    // Regex to parse the chunking directive
                    let chunk_directive_regex = Regex::new(r"@bro_chunk::(\d+)").unwrap();

                    // Check for the chunking directive
                    if let Some(caps) = chunk_directive_regex.captures(&sql_query) {
                        dbg!(&sql_query);
                        let chunk_size: i64 =
                            caps.get(1).map_or(0, |m| m.as_str().parse().unwrap_or(0));
                        let mut offset = 0; // Start with no offset
                        let mut combined_builder: Option<CsvBuilder> = None;

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .to_string();
                        let base_query = base_query
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .trim()
                            .to_string();
                        //dbg!(&base_query);

                        loop {
                            // Wrap the entire original query in a subquery for proper pagination
                            let chunk_query = format!(
            "SELECT * FROM ({} ) AS SubQuery ORDER BY (SELECT NULL) OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
            base_query, offset, chunk_size
        );

                            let chunk_insight =
                                format!("Executing the below chunk query ...\n\n{}", &chunk_query);

                            println!();
                            print_insight_level_2(&chunk_insight);

                            // Fetch the chunk as a mutable CsvBuilder
                            let mut result = CsvBuilder::from_mssql_query(
                                &username,
                                &password,
                                &host,
                                &database,
                                &chunk_query,
                            )
                            .await?;

                            result.print_table();

                            // Check if the current chunk is empty to break the loop
                            if !result.has_data() {
                                break;
                            }

                            // Update offset for the next chunk
                            offset += chunk_size;

                            // Combine the current chunk with the previous results
                            match &mut combined_builder {
                                Some(builder) => {
                                    // Pass the mutable reference of `result`
                                    builder
                                        .set_union_with_csv_builder(
                                            &mut result,
                                            "UNION_TYPE:NORMAL",
                                            vec!["*"],
                                        )
                                        .print_table();
                                }
                                None => combined_builder = Some(result),
                            }
                        }

                        // Finalize the combined result
                        query_execution_result =
                            combined_builder.ok_or_else(|| "No data fetched".into());
                    } else {
                        // Execute the query normally
                        query_execution_result = CsvBuilder::from_mssql_query(
                            &username, &password, &host, &database, &sql_query,
                        )
                        .await;
                    }

                    let elapsed_time = start_time.elapsed();

                    if let Err(e) = query_execution_result {
                        println!("Failed to execute query: {}", e);

                        let menu_options = vec!["TINKER", "SEARCH", "INSPECT", "PIVOT", "JOIN"];

                        print_list(&menu_options);
                        let choice = get_user_input("Enter your choice: ").to_lowercase();
                        confirmation = choice.clone();

                        if handle_query_special_flag(&choice, &mut csv_builder) {
                            //continue;
                            break Ok(CsvBuilder::new());
                        }

                        if handle_back_flag(&choice) {
                            //break;
                            break Ok(CsvBuilder::new());
                        }
                        let _ = handle_quit_flag(&choice);

                        if handle_query_retry_flag(&choice) {
                            continue;
                        }
                    } else {
                        csv_builder = query_execution_result.unwrap();
                        csv_builder.print_table(); // Print the table on success
                        println!("Executiom Time: {:?}", elapsed_time);
                        confirmation = String::new(); // Reset confirmation for the next loop iteration
                    }
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

                if confirmation == "TINKER"
                    || confirmation == "SEARCH"
                    || confirmation == "INSPECT"
                    || confirmation == "PIVOT"
                    || confirmation == "JOIN"
                {
                    csv_builder.print_table();
                    confirmation = String::new();
                } else {
                    let sql_query = if confirmation == "@r" && !last_sql_query.is_empty() {
                        // Use vim_edit only if confirmation is "retry"
                        let new_query = get_edited_user_sql_input(last_sql_query.clone());
                        last_sql_query = new_query.clone();
                        new_query
                    } else {
                        // Get new query from user, except when confirmation is "inspect"
                        let new_query = get_user_sql_input();
                        last_sql_query = new_query.clone();
                        new_query
                    };

                    let start_time = Instant::now();

                    let query_execution_result: Result<CsvBuilder, Box<dyn std::error::Error>>;

                    // Regex to parse the chunking directive
                    let chunk_directive_regex = Regex::new(r"@bro_chunk::(\d+)").unwrap();

                    // Check for the chunking directive
                    if let Some(caps) = chunk_directive_regex.captures(&sql_query) {
                        //dbg!(&sql_query);
                        let chunk_size: i64 =
                            caps.get(1).map_or(0, |m| m.as_str().parse().unwrap_or(0));
                        let mut offset = 0; // Start with no offset
                        let mut combined_builder: Option<CsvBuilder> = None;

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .to_string();
                        let base_query = base_query
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .trim()
                            .to_string();
                        //dbg!(&base_query);

                        loop {
                            // Use MySQL's LIMIT and OFFSET for pagination
                            let chunk_query = format!(
                                "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}",
                                base_query, chunk_size, offset
                            );

                            //dbg!(&chunk_query);
                            let chunk_insight =
                                format!("Executing the below chunk query ...\n\n{}", &chunk_query);

                            println!();
                            print_insight_level_2(&chunk_insight);

                            // Fetch the chunk as a mutable CsvBuilder
                            let mut result = CsvBuilder::from_mysql_query(
                                &username,
                                &password,
                                &host,
                                &database,
                                &chunk_query,
                            )
                            .await?;

                            result.print_table();

                            // Check if the current chunk is empty to break the loop
                            if !result.has_data() {
                                break;
                            }

                            // Update offset for the next chunk
                            offset += chunk_size;

                            // Combine the current chunk with the previous results
                            match &mut combined_builder {
                                Some(builder) => {
                                    // Pass the mutable reference of `result`
                                    builder
                                        .set_union_with_csv_builder(
                                            &mut result,
                                            "UNION_TYPE:NORMAL",
                                            vec!["*"],
                                        )
                                        .print_table();
                                }
                                None => combined_builder = Some(result),
                            }
                        }

                        // Finalize the combined result
                        query_execution_result =
                            combined_builder.ok_or_else(|| "No data fetched".into());
                    } else {
                        // Execute the query normally
                        query_execution_result = CsvBuilder::from_mysql_query(
                            &username, &password, &host, &database, &sql_query,
                        )
                        .await;
                    }

                    let elapsed_time = start_time.elapsed();

                    if let Err(e) = query_execution_result {
                        println!("Failed to execute query: {}", e);

                        let menu_options = vec!["TINKER", "SEARCH", "INSPECT", "PIVOT", "JOIN"];

                        print_list(&menu_options);
                        let choice = get_user_input("Enter your choice: ").to_lowercase();
                        confirmation = choice.clone();

                        if handle_query_special_flag(&choice, &mut csv_builder) {
                            //continue;
                            break Ok(CsvBuilder::new());
                        }

                        if handle_back_flag(&choice) {
                            //break;
                            break Ok(CsvBuilder::new());
                        }
                        let _ = handle_quit_flag(&choice);

                        if handle_query_retry_flag(&choice) {
                            continue;
                        }
                    } else {
                        csv_builder = query_execution_result.unwrap();
                        csv_builder.print_table(); // Print the table on success
                        println!("Executiom Time: {:?}", elapsed_time);
                        confirmation = String::new(); // Reset confirmation for the next loop iteration
                    }
                }
            }
        };

        println!();

        let menu_options = vec!["TINKER", "SEARCH", "INSPECT", "PIVOT", "JOIN"];

        print_list(&menu_options);
        let choice = get_user_input("Enter your choice: ").to_lowercase();

        if handle_query_special_flag(&choice, &mut csv_builder) {
            //continue;
            break Ok(CsvBuilder::new());
        }

        if handle_back_flag(&choice) {
            //break;
            break Ok(CsvBuilder::new());
        }
        let _ = handle_quit_flag(&choice);

        if handle_query_retry_flag(&choice) {
            confirmation = "@r".to_string();
            continue;
        }

        let selected_option = determine_action_as_text(&menu_options, &choice);
        confirmation = selected_option.clone().expect("REASON");

        match selected_option {
            Some(ref action) if action == "TINKER" => {
                if let Err(e) = handle_tinker(&mut csv_builder, None).await {
                    println!("Error during tinker: {}", e);
                    continue;
                }
            }

            Some(ref action) if action == "SEARCH" => {
                if let Err(e) = handle_search(&mut csv_builder, None).await {
                    println!("Error during search: {}", e);
                    continue;
                }
            }

            Some(ref action) if action == "INSPECT" => {
                if let Err(e) = handle_inspect(&mut csv_builder, None) {
                    println!("Error during inspection: {}", e);
                    continue;
                }
            }

            Some(ref action) if action == "PIVOT" => {
                if let Err(e) = handle_pivot(&mut csv_builder, None).await {
                    println!("Error during pivot operation: {}", e);
                    continue;
                }
            }

            Some(ref action) if action == "JOIN" => {
                if let Err(e) = handle_join(&mut csv_builder, None) {
                    println!("Error during join operation: {}", e);
                    continue;
                }
            }

            None => todo!(),
            Some(_) => todo!(),
        }
    }
}

pub async fn chain_builder(mut builder: CsvBuilder, file_path_option: Option<&str>) {
    //let current_file_path: Option<PathBuf> = file_path_option.map(PathBuf::from);

    if builder.has_data() {
        let _ = builder.print_table();
        println!();
    }

    //let home_dir = env::var("HOME").expect("Unable to determine user home directory");
    //let desktop_path = Path::new(&home_dir).join("Desktop");
    //let csv_db_path = desktop_path.join("csv_db");

    loop {
        //let has_data = builder.has_data();
        print_insight("Choose an action:");

        let menu_options = vec!["TINKER", "SEARCH", "INSPECT", "PIVOT", "JOIN"];

        print_list(&menu_options);
        let choice = get_user_input("Enter your choice: ").to_lowercase();

        if handle_special_flag(&choice, &mut builder, file_path_option) {
            continue;
        }

        if handle_back_flag(&choice) {
            break;
        }
        let _ = handle_quit_flag(&choice);

        let selected_option = determine_action_as_text(&menu_options, &choice);

        match selected_option {
            Some(ref action) if action == "TINKER" => {
                if let Err(e) = handle_tinker(&mut builder, file_path_option).await {
                    println!("Error during tinker: {}", e);
                    continue;
                }
            }

            Some(ref action) if action == "SEARCH" => {
                if let Err(e) = handle_search(&mut builder, file_path_option).await {
                    println!("Error during search: {}", e);
                    continue;
                }
            }

            Some(ref action) if action == "INSPECT" => {
                if let Err(e) = handle_inspect(&mut builder, file_path_option) {
                    println!("Error during inspection: {}", e);
                    continue;
                }
            }

            Some(ref action) if action == "PIVOT" => {
                if let Err(e) = handle_pivot(&mut builder, file_path_option).await {
                    println!("Error during pivot operation: {}", e);
                    continue;
                }
            }

            Some(ref action) if action == "JOIN" => {
                if let Err(e) = handle_join(&mut builder, file_path_option) {
                    println!("Error during join operation: {}", e);
                    continue;
                }
            }
            //"done" => break,
            Some(_) => print_insight("Unrecognized action, please try again."),
            None => print_insight("No action determined"),
        }
    }
}
