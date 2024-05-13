// db_connector.rs
use crate::config::{Config, DbPreset};
use crate::csv_inspector::handle_inspect;
use crate::csv_joiner::handle_join;
use crate::csv_pivoter::handle_pivot;
use crate::csv_searcher::handle_search;
use crate::csv_tinkerer::handle_tinker;
use crate::user_experience::{
    handle_back_flag, handle_cancel_flag, handle_query_retry_flag, handle_query_special_flag,
    handle_quit_flag,
};
use crate::user_interaction::{
    determine_action_as_number, determine_action_as_text, get_edited_user_sql_input,
    get_user_input, get_user_input_level_2, get_user_sql_input, print_list,
};
use regex::Regex;
use rgwml::csv_utils::CsvBuilder;
use rgwml::db_utils::DbConnect;
use serde_json::from_str;
use std::error::Error;
use std::fs::read_to_string;
use std::path::PathBuf;
use std::time::Instant;

enum DbType {
    MsSql,
    MySql,
}

#[allow(unused_assignments)]
pub async fn query(csv_db_path: &PathBuf) -> Result<CsvBuilder, Box<dyn std::error::Error>> {
    fn get_db_type(
        csv_db_path: &PathBuf,
    ) -> Result<(DbType, Option<DbPreset>), Box<dyn std::error::Error>> {
        let config_path = csv_db_path.join("bro.config");
        let file_contents = read_to_string(config_path)?;
        let valid_json_part = file_contents
            .split("SYNTAX")
            .next()
            .ok_or("Invalid configuration format")?;
        let config: Config = from_str(valid_json_part)?;

        let presets = config.db_presets;
        let options: Vec<&str> = presets.iter().map(|preset| preset.name.as_str()).collect();

        print_list(&options);
        let choice = get_user_input_level_2("Choose a database: ").to_lowercase();
        let selected_option = determine_action_as_number(&options, &choice);

        if let Some(serial) = selected_option {
            if serial > 0 && serial <= presets.len() {
                let preset = &presets[serial - 1];
                let db_type = match preset.db_type.to_lowercase().as_str() {
                    "mssql" => DbType::MsSql,
                    "mysql" => DbType::MySql,
                    _ => {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Unknown database type in preset",
                        )) as Box<dyn Error>)
                    }
                };
                return Ok((db_type, Some(preset.clone())));
            }
        }

        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Invalid selection",
        )) as Box<dyn Error>)
    }

    let (db_type, preset_option) = match get_db_type(csv_db_path) {
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

                    let start_time = Instant::now();
                    let query_execution_result: Result<CsvBuilder, Box<dyn std::error::Error>>;

                    // Regex to parse the chunking directive
                    let chunk_directive_regex = Regex::new(r"@bro_chunk::(\d+)").unwrap();
                    let show_architecture_directive_regex = Regex::new(r"^@bro_show_all").unwrap();
                    let show_databases_directive_regex =
                        Regex::new(r"^@bro_show_databases").unwrap();
                    //let show_schemas_directive_regex = Regex::new(r"@bro_show_schemas::(\d+)").unwrap();
                    let show_schemas_directive_regex =
                        Regex::new(r"@bro_show_schemas::(\w+)").unwrap();

                    //let show_tables_directive_regex = Regex::new(r"@bro_show_tables::([^.]+)\.(\w+)").unwrap();
                    let show_tables_directive_regex =
                        Regex::new(r"@bro_show_tables::([^.]+)(?:\.(\w+))?").unwrap();
                    //let describe_directive_regex = Regex::new(r"@bro_describe::(\w+)").unwrap();
                    let describe_directive_regex =
                        Regex::new(r"@bro_describe::(?:([^.\s]+)\.)?(?:([^.\s]+)\.)?(\w+)")
                            .unwrap();

                    // Check for the chunking directive
                    if handle_cancel_flag(&sql_query) {
                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(caps) = chunk_directive_regex.captures(&sql_query) {
                        let chunk_size = caps.get(1).unwrap().as_str();

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .to_string();

                        //dbg!(&base_query);

                        // Execute the chunked query using the newly created method
                        query_execution_result = CsvBuilder::from_chunked_mssql_query(
                            &username,
                            &password,
                            &host,
                            &database,
                            &base_query,
                            chunk_size,
                        )
                        .await;
                    } else if let Some(_) = show_architecture_directive_regex.captures(&sql_query) {
                        let _ = DbConnect::print_mssql_architecture(
                            &username, &password, &host, &database,
                        )
                        .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(_) = show_databases_directive_regex.captures(&sql_query) {
                        let _ = DbConnect::print_mssql_databases(
                            &username, &password, &host, &database,
                        )
                        .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(caps) = show_schemas_directive_regex.captures(&sql_query) {
                        let in_focus_database = caps.get(1).unwrap().as_str();

                        let _ = DbConnect::print_mssql_schemas(
                            &username,
                            &password,
                            &host,
                            in_focus_database,
                        )
                        .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(caps) = show_tables_directive_regex.captures(&sql_query) {
                        let in_focus_database = caps.get(1).unwrap().as_str();
                        let schema = caps.get(2).map_or("", |m| m.as_str());

                        let _ = DbConnect::print_mssql_tables(
                            &username,
                            &password,
                            &host,
                            in_focus_database,
                            schema,
                        )
                        .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(caps) = describe_directive_regex.captures(&sql_query) {
                        let specified_database =
                            caps.get(1).map_or(database.as_str(), |m| m.as_str());
                        let _schema = caps.get(2).map_or("dbo", |m| m.as_str());
                        let table_name = caps.get(3).unwrap().as_str();

                        let result = CsvBuilder::get_mssql_table_description(
                            &username,
                            &password,
                            &host,
                            &specified_database,
                            //schema,
                            table_name,
                        )
                        .await;

                        query_execution_result = Ok(result?);
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
                        if csv_builder.has_data() && csv_builder.has_headers() {
                            csv_builder.print_table(); // Print the table on success
                        }
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
                    let show_architecture_directive_regex = Regex::new(r"^@bro_show_all").unwrap();
                    let show_databases_directive_regex =
                        Regex::new(r"^@bro_show_databases").unwrap();
                    let show_tables_directive_regex =
                        Regex::new(r"@bro_show_tables::([^\s]+)").unwrap();
                    let describe_directive_regex =
                        Regex::new(r"@bro_describe::(?:([^.\s]+)\.)?(\w+)").unwrap();

                    if handle_cancel_flag(&sql_query) {
                        query_execution_result = Ok(CsvBuilder::new());
                    }
                    // Check for the chunking directive
                    else if let Some(caps) = chunk_directive_regex.captures(&sql_query) {
                        let chunk_size = caps.get(1).unwrap().as_str(); // Directly use the captured string

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .to_string();

                        //dbg!(&base_query);

                        // Execute the chunked query using the newly created method
                        query_execution_result = CsvBuilder::from_chunked_mysql_query(
                            &username,
                            &password,
                            &host,
                            &database,
                            &base_query,
                            chunk_size,
                        )
                        .await;
                    } else if let Some(_) = show_architecture_directive_regex.captures(&sql_query) {
                        dbg!(&sql_query);
                        let _ = DbConnect::print_mysql_architecture(
                            &username, &password, &host, &database,
                        )
                        .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(_) = show_databases_directive_regex.captures(&sql_query) {
                        let _ = DbConnect::print_mysql_databases(
                            &username, &password, &host, &database,
                        )
                        .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(caps) = show_tables_directive_regex.captures(&sql_query) {
                        let in_focus_database = caps.get(1).unwrap().as_str();

                        let _ = DbConnect::print_mysql_tables(
                            &username,
                            &password,
                            &host,
                            in_focus_database,
                        )
                        .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(caps) = describe_directive_regex.captures(&sql_query) {
                        // Extract database and table name from the captures
                        let specified_database =
                            caps.get(1).map_or(database.as_str(), |m| m.as_str());
                        let table_name = caps.get(2).unwrap().as_str();
                        let result = CsvBuilder::get_mysql_table_description(
                            &username,
                            &password,
                            &host,
                            &specified_database,
                            //schema,
                            table_name,
                        )
                        .await;

                        query_execution_result = Ok(result?);
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

                        if csv_builder.has_data() && csv_builder.has_headers() {
                            csv_builder.print_table(); // Print the table on success
                        }

                        //csv_builder.print_table(); // Print the table on success
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