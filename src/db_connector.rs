// db_connector.rs
use crate::config::{Config, DbPreset, GoogleBigQueryPreset};
use crate::csv_appender::handle_append;
use crate::csv_inspector::handle_inspect;
use crate::csv_joiner::handle_join;
use crate::csv_predicter::handle_predict;
use crate::csv_searcher::handle_search;
use crate::csv_tinkerer::handle_tinker;
use crate::csv_transformer::handle_transform;
use crate::user_experience::{
    handle_back_flag, handle_cancel_flag, handle_query_retry_flag, handle_query_special_flag,
    handle_quit_flag,
};
use crate::user_interaction::{
    determine_action_as_number, determine_action_as_text, get_edited_user_sql_input,
    get_user_input, get_user_input_level_2, print_list,
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
    ClickHouse,
    GoogleBigQuery,
}

#[allow(unused_assignments)]
pub async fn query(csv_db_path: &PathBuf) -> Result<CsvBuilder, Box<dyn std::error::Error>> {
    fn get_db_type(
        csv_db_path: &PathBuf,
    ) -> Result<(DbType, Option<DbPreset>, Option<GoogleBigQueryPreset>), Box<dyn std::error::Error>>
    {
        let config_path = csv_db_path.join("bro.config");
        let file_contents = read_to_string(config_path)?;
        let valid_json_part = file_contents
            .split("SYNTAX")
            .next()
            .ok_or("Invalid configuration format")?;
        let config: Config = from_str(valid_json_part)?;

        let presets = config.db_presets;
        let google_presets = config.google_big_query_presets;

        let mut options: Vec<(usize, &str)> = presets
            .iter()
            .enumerate()
            .map(|(i, preset)| (i, preset.name.as_str()))
            .collect();
        options.extend(
            google_presets
                .iter()
                .enumerate()
                .map(|(i, preset)| (i + presets.len(), preset.name.as_str())),
        );

        // Sort the options alphabetically by the preset name
        options.sort_by(|a, b| a.1.cmp(b.1));

        // Print the sorted list of names
        print_list(&options.iter().map(|(_, name)| *name).collect::<Vec<_>>());

        let choice = get_user_input_level_2("Choose a database: ").to_lowercase();

        if handle_back_flag(&choice) {
            //break;
            // Handling the case where no valid option is selected
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "return_to_main",
            )) as Box<dyn Error>);
        }

        let selected_option = determine_action_as_number(
            &options.iter().map(|(_, name)| *name).collect::<Vec<_>>(),
            &choice,
        );

        //dbg!(&options, &choice, &selected_option);

        if let Some(serial) = selected_option {
            if serial > 0 && serial <= options.len() {
                let original_index = options[serial - 1].0;

                if original_index < presets.len() {
                    let preset = &presets[original_index];
                    let db_type = match preset.db_type.to_lowercase().as_str() {
                        "mssql" => DbType::MsSql,
                        "mysql" => DbType::MySql,
                        "clickhouse" => DbType::ClickHouse,
                        "googlebigquery" => DbType::GoogleBigQuery,
                        _ => {
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "Unknown database type in preset",
                            )) as Box<dyn Error>)
                        }
                    };
                    return Ok((db_type, Some(preset.clone()), None));
                } else {
                    let google_preset_index = original_index - presets.len();
                    let google_preset = &google_presets[google_preset_index];
                    return Ok((DbType::GoogleBigQuery, None, Some(google_preset.clone())));
                }
            }
        }

        // Handling the case where no valid option is selected
        Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Invalid selection",
        )) as Box<dyn Error>)
    }

    let syntax = r#"

DIRECTIVES SYNTAX
=================
@bro_show_all
@bro_show_databases
@bro_show_schemas::your_db_name   // For mssql only
@bro_show_tables::your_db_name
@bro_chunk_union::number_of_rows_to_chunk_by { SELECT * FROM your_table }

    /* This query should be simple and should not include LIMIT, OFFSET, or 
     * ORDER BY clauses as these will be dynamically applied to manage data chunking. 
     * Ensure that the `sql_query` does not include any complex subqueries or joins 
     * that might interfere with this limit-offset pagination mechanism - 
     * "SELECT * FROM ({}) AS SubQuery LIMIT {} OFFSET {}" */

@bro_chunk_bag_union::number_of_rows_to_chunk_by { SELECT * FROM your_table }

    /* Same as above, except that duplicate rows are not removed as the chunks
     * are received */

@bro_describe::your_table_name
        "#;

    let (db_type, db_preset_option, google_preset_option) = match get_db_type(csv_db_path) {
        Ok(db) => db,
        Err(e) => {
            if e.to_string() == "return_to_main" {
                return Err("User chose to go back".into());
            } else {
                return Err(e);
            }
        }
    };

    // Further handling of the presets based on the selected db_type
    let (username, password, host, database, json_file_path, project_id) =
        if let Some(preset) = db_preset_option {
            (
                preset.username,
                preset.password,
                preset.host,
                preset.database,
                String::new(),
                String::new(),
            )
        } else if let Some(google_preset) = google_preset_option {
            (
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                google_preset.json_file_path,
                google_preset.project_id,
            )
        } else {
            (
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
            )
        };

    // dbg!(&username, &password, &host, &database, &json_file_path, &project_id);

    //let mut csv_builder: CsvBuilder;
    let mut csv_builder: CsvBuilder = CsvBuilder::new();
    let mut last_sql_query = String::new();
    let mut confirmation = String::new();

    let special_confirmations = vec![
        "SEARCH",
        "INSPECT",
        "TINKER",
        "TRANSFORM",
        "APPEND",
        "JOIN",
        "PREDICT",
    ];

    loop {
        let _query_result: Result<CsvBuilder, Box<dyn std::error::Error>>;

        match db_type {
            DbType::MsSql => {
                // Existing connection logic for i2e1
                if special_confirmations.contains(&confirmation.as_str()) {
                    csv_builder.print_table();
                    confirmation = String::new();
                } else {
                    let sql_query = if confirmation == "@r" && !last_sql_query.is_empty() {
                        // Use vim_edit only if confirmation is "retry"
                        let last_sql_query_with_appended_syntax = last_sql_query.clone() + syntax;

                        let new_query =
                            get_edited_user_sql_input(last_sql_query_with_appended_syntax);
                        last_sql_query = new_query.clone();
                        new_query
                    } else {
                        // Get new query from user, except when confirmation is "inspect"

                        let new_query = get_edited_user_sql_input(syntax.to_string());
                        last_sql_query = new_query.clone();
                        new_query
                    };

                    let start_time = Instant::now();
                    let mut is_table_description = false;
                    let query_execution_result: Result<CsvBuilder, Box<dyn std::error::Error>>;

                    // Regex to parse the chunking directive
                    let chunk_union_directive_regex =
                        Regex::new(r"@bro_chunk_union::(\d+)").unwrap();
                    let chunk_bag_union_directive_regex =
                        Regex::new(r"@bro_chunk_bag_union::(\d+)").unwrap();

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
                    } else if let Some(caps) = chunk_union_directive_regex.captures(&sql_query) {
                        let chunk_size = caps.get(1).unwrap().as_str();

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_union_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .to_string();

                        //dbg!(&base_query);

                        // Execute the chunked query using the newly created method
                        query_execution_result = CsvBuilder::from_chunked_mssql_query_union(
                            &username,
                            &password,
                            &host,
                            &database,
                            &base_query,
                            chunk_size,
                        )
                        .await;
                    } else if let Some(caps) = chunk_bag_union_directive_regex.captures(&sql_query)
                    {
                        let chunk_size = caps.get(1).unwrap().as_str();

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_bag_union_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .to_string();

                        //dbg!(&base_query);

                        // Execute the chunked query using the newly created method
                        query_execution_result = CsvBuilder::from_chunked_mssql_query_bag_union(
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

                        is_table_description = true;

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

                        //let menu_options =
                        //  vec!["TINKER", "SEARCH", "INSPECT", "JOIN", "GROUP", "PIVOT"];
                        print_list(&special_confirmations);
                        //print_list(&menu_options);
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

                        if is_table_description {
                            csv_builder.print_table_all_rows();
                        } else if csv_builder.has_data() && csv_builder.has_headers() {
                            csv_builder.print_table(); // Print the table on success
                        }
                        println!("Executiom Time: {:?}", elapsed_time);
                        confirmation = String::new(); // Reset confirmation for the next loop iteration
                    }
                }
            }

            DbType::MySql => {
                // Existing connection logic for i2e1
                if special_confirmations.contains(&confirmation.as_str()) {
                    csv_builder.print_table();
                    confirmation = String::new();
                } else {
                    let sql_query = if confirmation == "@r" && !last_sql_query.is_empty() {
                        // Use vim_edit only if confirmation is "retry"
                        let last_sql_query_with_appended_syntax = last_sql_query.clone() + syntax;

                        let new_query =
                            get_edited_user_sql_input(last_sql_query_with_appended_syntax);
                        last_sql_query = new_query.clone();
                        new_query
                    } else {
                        // Get new query from user, except when confirmation is "inspect"

                        let new_query = get_edited_user_sql_input(syntax.to_string());
                        last_sql_query = new_query.clone();
                        new_query
                    };

                    let mut is_table_description = false;
                    let start_time = Instant::now();

                    let query_execution_result: Result<CsvBuilder, Box<dyn std::error::Error>>;

                    // Regex to parse the chunking directive
                    // Regex to parse the chunking directive
                    let chunk_union_directive_regex =
                        Regex::new(r"@bro_chunk_union::(\d+)").unwrap();
                    let chunk_bag_union_directive_regex =
                        Regex::new(r"@bro_chunk_bag_union::(\d+)").unwrap();

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
                    else if let Some(caps) = chunk_union_directive_regex.captures(&sql_query) {
                        let chunk_size = caps.get(1).unwrap().as_str(); // Directly use the captured string

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_union_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .to_string();

                        //dbg!(&base_query);

                        // Execute the chunked query using the newly created method
                        query_execution_result = CsvBuilder::from_chunked_mysql_query_union(
                            &username,
                            &password,
                            &host,
                            &database,
                            &base_query,
                            chunk_size,
                        )
                        .await;
                    } else if let Some(caps) = chunk_bag_union_directive_regex.captures(&sql_query)
                    {
                        let chunk_size = caps.get(1).unwrap().as_str(); // Directly use the captured string

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_bag_union_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .to_string();

                        //dbg!(&base_query);

                        // Execute the chunked query using the newly created method
                        query_execution_result = CsvBuilder::from_chunked_mysql_query_bag_union(
                            &username,
                            &password,
                            &host,
                            &database,
                            &base_query,
                            chunk_size,
                        )
                        .await;
                    } else if let Some(_) = show_architecture_directive_regex.captures(&sql_query) {
                        //dbg!(&sql_query);
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

                        is_table_description = true;

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

                        //let menu_options =
                        //   vec!["TINKER", "SEARCH", "INSPECT", "JOIN", "GROUP", "PIVOT"];
                        print_list(&special_confirmations);
                        //print_list(&menu_options);
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

                        if is_table_description {
                            csv_builder.print_table_all_rows();
                        } else if csv_builder.has_data() && csv_builder.has_headers() {
                            csv_builder.print_table(); // Print the table on success
                        }

                        //csv_builder.print_table(); // Print the table on success
                        println!("Executiom Time: {:?}", elapsed_time);
                        confirmation = String::new(); // Reset confirmation for the next loop iteration
                    }
                }
            }

            DbType::ClickHouse => {
                // Existing connection logic for i2e1
                if special_confirmations.contains(&confirmation.as_str()) {
                    csv_builder.print_table();
                    confirmation = String::new();
                } else {
                    let sql_query = if confirmation == "@r" && !last_sql_query.is_empty() {
                        // Use vim_edit only if confirmation is "retry"
                        let last_sql_query_with_appended_syntax = last_sql_query.clone() + syntax;

                        let new_query =
                            get_edited_user_sql_input(last_sql_query_with_appended_syntax);
                        last_sql_query = new_query.clone();
                        new_query
                    } else {
                        // Get new query from user, except when confirmation is "inspect"

                        let new_query = get_edited_user_sql_input(syntax.to_string());
                        last_sql_query = new_query.clone();
                        new_query
                    };

                    let mut is_table_description = false;
                    let start_time = Instant::now();

                    let query_execution_result: Result<CsvBuilder, Box<dyn std::error::Error>>;

                    // Regex to parse the chunking directive
                    //let chunk_directive_regex = Regex::new(r"@bro_chunk::(\d+)").unwrap();

                    let chunk_union_directive_regex =
                        Regex::new(r"@bro_chunk_union::(\d+)").unwrap();
                    let chunk_bag_union_directive_regex =
                        Regex::new(r"@bro_chunk_bag_union::(\d+)").unwrap();

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
                    else if let Some(caps) = chunk_union_directive_regex.captures(&sql_query) {
                        let chunk_size = caps.get(1).unwrap().as_str(); // Directly use the captured string

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_union_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .to_string();

                        //dbg!(&base_query);

                        // Execute the chunked query using the newly created method
                        query_execution_result = CsvBuilder::from_chunked_clickhouse_query_union(
                            &username,
                            &password,
                            &host,
                            &base_query,
                            chunk_size,
                        )
                        .await;
                    } else if let Some(caps) = chunk_bag_union_directive_regex.captures(&sql_query)
                    {
                        let chunk_size = caps.get(1).unwrap().as_str(); // Directly use the captured string

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_bag_union_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .to_string();

                        //dbg!(&base_query);

                        // Execute the chunked query using the newly created method
                        query_execution_result =
                            CsvBuilder::from_chunked_clickhouse_query_bag_union(
                                &username,
                                &password,
                                &host,
                                &base_query,
                                chunk_size,
                            )
                            .await;
                    } else if let Some(_) = show_architecture_directive_regex.captures(&sql_query) {
                        //dbg!(&sql_query);
                        let _ =
                            DbConnect::print_clickhouse_architecture(&username, &password, &host)
                                .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(_) = show_databases_directive_regex.captures(&sql_query) {
                        let _ = DbConnect::print_clickhouse_databases(&username, &password, &host)
                            .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(caps) = show_tables_directive_regex.captures(&sql_query) {
                        let in_focus_database = caps.get(1).unwrap().as_str();

                        let _ = DbConnect::print_clickhouse_tables(
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
                        let result = CsvBuilder::get_clickhouse_table_description(
                            &username,
                            &password,
                            &host,
                            &specified_database,
                            //schema,
                            table_name,
                        )
                        .await;

                        is_table_description = true;

                        query_execution_result = Ok(result?);
                    } else {
                        // Execute the query normally
                        query_execution_result = CsvBuilder::from_clickhouse_query(
                            &username, &password, &host, &sql_query,
                        )
                        .await;
                    }

                    let elapsed_time = start_time.elapsed();

                    if let Err(e) = query_execution_result {
                        println!("Failed to execute query: {}", e);

                        //let menu_options =
                        //  vec!["TINKER", "SEARCH", "INSPECT", "JOIN", "GROUP", "PIVOT"];
                        print_list(&special_confirmations);
                        //print_list(&menu_options);
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

                        if is_table_description {
                            csv_builder.print_table_all_rows();
                        } else if csv_builder.has_data() && csv_builder.has_headers() {
                            csv_builder.print_table(); // Print the table on success
                        }

                        //csv_builder.print_table(); // Print the table on success
                        println!("Executiom Time: {:?}", elapsed_time);
                        confirmation = String::new(); // Reset confirmation for the next loop iteration
                    }
                }
            }

            DbType::GoogleBigQuery => {
                // Existing connection logic for i2e1
                if special_confirmations.contains(&confirmation.as_str()) {
                    csv_builder.print_table();
                    confirmation = String::new();
                } else {
                    let sql_query = if confirmation == "@r" && !last_sql_query.is_empty() {
                        // Use vim_edit only if confirmation is "retry"
                        let last_sql_query_with_appended_syntax = last_sql_query.clone() + syntax;

                        let new_query =
                            get_edited_user_sql_input(last_sql_query_with_appended_syntax);
                        last_sql_query = new_query.clone();
                        new_query
                    } else {
                        // Get new query from user, except when confirmation is "inspect"

                        let new_query = get_edited_user_sql_input(syntax.to_string());
                        last_sql_query = new_query.clone();
                        new_query
                    };

                    let mut is_table_description = false;
                    let start_time = Instant::now();

                    let query_execution_result: Result<CsvBuilder, Box<dyn std::error::Error>>;

                    // Regex to parse the chunking directive
                    //let chunk_directive_regex = Regex::new(r"@bro_chunk::(\d+)").unwrap();

                    let chunk_union_directive_regex =
                        Regex::new(r"@bro_chunk_union::(\d+)").unwrap();
                    let chunk_bag_union_directive_regex =
                        Regex::new(r"@bro_chunk_bag_union::(\d+)").unwrap();

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
                    else if let Some(caps) = chunk_union_directive_regex.captures(&sql_query) {
                        let chunk_size = caps.get(1).unwrap().as_str(); // Directly use the captured string

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_union_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .to_string();

                        //dbg!(&base_query);

                        // Execute the chunked query using the newly created method
                        query_execution_result =
                            CsvBuilder::from_chunked_google_big_query_query_union(
                                &json_file_path,
                                &base_query,
                                chunk_size,
                            )
                            .await;
                    } else if let Some(caps) = chunk_bag_union_directive_regex.captures(&sql_query)
                    {
                        let chunk_size = caps.get(1).unwrap().as_str(); // Directly use the captured string

                        // Remove the chunk directive and trim extra characters
                        let base_query = chunk_bag_union_directive_regex
                            .replace(&sql_query, "")
                            .trim()
                            .trim_matches(|c: char| c == '{' || c == '}')
                            .to_string();

                        //dbg!(&base_query);

                        // Execute the chunked query using the newly created method
                        query_execution_result =
                            CsvBuilder::from_chunked_google_big_query_query_bag_union(
                                &json_file_path,
                                &base_query,
                                chunk_size,
                            )
                            .await;
                    } else if let Some(_) = show_architecture_directive_regex.captures(&sql_query) {
                        //dbg!(&sql_query);

                        let _ = DbConnect::print_google_big_query_architecture(
                            &json_file_path,
                            &project_id,
                        )
                        .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(_) = show_databases_directive_regex.captures(&sql_query) {
                        let _ = DbConnect::print_google_big_query_datasets(
                            &json_file_path,
                            &project_id,
                        )
                        .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(caps) = show_tables_directive_regex.captures(&sql_query) {
                        let dataset = caps.get(1).unwrap().as_str();

                        let _ = DbConnect::print_google_big_query_tables(
                            &json_file_path,
                            &project_id,
                            dataset,
                        )
                        .await;

                        query_execution_result = Ok(CsvBuilder::new());
                    } else if let Some(caps) = describe_directive_regex.captures(&sql_query) {
                        // Extract database and table name from the captures
                        let specified_dataset =
                            caps.get(1).map_or(database.as_str(), |m| m.as_str());
                        let table_name = caps.get(2).unwrap().as_str();
                        let result = CsvBuilder::get_google_big_query_table_description(
                            &json_file_path,
                            &project_id,
                            &specified_dataset,
                            table_name,
                        )
                        .await;

                        is_table_description = true;

                        query_execution_result = Ok(result?);
                    } else {
                        // Execute the query normally
                        query_execution_result =
                            CsvBuilder::from_google_big_query_query(&json_file_path, &sql_query)
                                .await;
                    }

                    let elapsed_time = start_time.elapsed();

                    if let Err(e) = query_execution_result {
                        println!("Failed to execute query: {}", e);

                        //let menu_options =
                        //  vec!["TINKER", "SEARCH", "INSPECT", "JOIN", "GROUP", "PIVOT"];

                        print_list(&special_confirmations);
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

                        if is_table_description {
                            csv_builder.print_table_all_rows();
                        } else if csv_builder.has_data() && csv_builder.has_headers() {
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

        //let menu_options = vec!["TINKER", "SEARCH", "INSPECT", "JOIN", "GROUP", "PIVOT"];

        print_list(&special_confirmations);
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

        let selected_option = determine_action_as_text(&special_confirmations, &choice);
        confirmation = selected_option.clone().expect("REASON");

        match selected_option {
            Some(ref action) if action == "TINKER" => {
                if let Err(e) = handle_tinker(&mut csv_builder, None, "1", "d").await {
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

            Some(ref action) if action == "APPEND" => {
                if let Err(e) = handle_append(&mut csv_builder, None).await {
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

            Some(ref action) if action == "TRANSFORM" => {
                if let Err(e) = handle_transform(&mut csv_builder, None).await {
                    println!("Error during group operation: {}", e);
                    continue;
                }
            }

            Some(ref action) if action == "PREDICT" => {
                if let Err(e) = handle_predict(&mut csv_builder, None).await {
                    println!("Error during group operation: {}", e);
                    continue;
                }
            }

            None => todo!(),
            Some(_) => todo!(),
        }
    }
}
