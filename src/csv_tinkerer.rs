// csv_tinkerer.rs
use crate::user_experience::{
    handle_back_flag, handle_cancel_flag, handle_quit_flag, handle_special_flag,
};
use crate::user_interaction::{
    determine_action_as_number, get_edited_user_json_input, get_edited_user_sql_input,
    get_user_input_level_2, print_insight_level_2, print_list_level_2,
};
use rgwml::csv_utils::{CsvBuilder, Exp, ExpVal};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::error::Error;

struct ExpStore {
    expressions: Vec<Exp>, // Store the Exp instances directly
}

#[derive(Debug)]
enum CompareValue {
    Single(String),
    Multiple(Vec<String>),
}

impl ExpStore {
    fn add_expression(
        &mut self,
        column: String,
        operator: String,
        compare_value: CompareValue,
        compare_type: String,
    ) {
        let exp = match compare_value {
            CompareValue::Single(value) => Exp {
                column,
                operator,
                compare_with: ExpVal::STR(value),
                compare_as: compare_type,
            },
            CompareValue::Multiple(values) => Exp {
                column,
                operator,
                compare_with: ExpVal::VEC(values),
                compare_as: compare_type,
            },
        };

        self.expressions.push(exp);
    }

    fn get_exp(&self, index: usize) -> &Exp {
        &self.expressions[index]
    }
}

pub async fn handle_tinker(
    csv_builder: &mut CsvBuilder,
    file_path_option: Option<&str>,
    action: &str,
    doc_request_flag: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    fn get_filter_expressions(
        data_store: &mut ExpStore,
    ) -> Result<(Vec<(String, usize)>, String), Box<dyn std::error::Error>> {
        let syntax = r#"{

  "expressions": [
    [
      "Exp1",
      {
        "column": "",
        "operator": "",
        "compare_with": "",
        "compare_as": ""
      }
    ]
  ],
  "evaluation": ""
}

SYNTAX
======

1. MULTIPLE EXPRESSION TEMPLATE
-------------------------------

{
  "expressions": [
    [
      "Exp1",
      {
        "column": "",
        "operator": "",
        "compare_with": "",
        "compare_as": ""
      }
    ],
    [
      "Exp2",
      {
        "column": "",
        "operator": "",
        "compare_with": "",
        "compare_as": ""
      }
    ]
  ],
  "evaluation": "Exp1 && Exp2"
}

2. NUMBERS/TIMESTAMPS/TEXT Operations
------------------------------------

### Expression

    {
      "expressions": [
        [
          "Exp1",
          {
            "column": "customer_type"
            "operator": "==",
            "compare_with": "PROSPECT",
            "compare_as": "TEXT"
          }
        ],
        [
          "Exp2",
          {
            "column": "added_at"
            "operator": ">",
            "compare_with": "2024-01-01 00:00:00",
            "compare_as": "TIMESTAMPS"
          }
        ],
        [,
          "Exp3",
          {
            "column": "invoice_amount"
            "operator": "<=",
            "compare_with": "5000",
            "compare_as": "NUMBERS"
          }
        ]
      ],
      "evaluation": "Exp1 && (Exp2 || Exp3)"
    }

### Available Operators

- NUMBERS/TIMESTAMPS (==, !=, >, <, >=, <=)
- TEXT (==, !=, CONTAINS, STARTS_WITH, DOES_NOT_CONTAIN)

3. VECTOR/ARRAY Operations
--------------------------

### Expression

    {
      "expressions": [
        [
          "Exp1",
          {
            "column": "address"
            "operator": "FUZZ_MIN_SCORE_60",
            "compare_with": [
                "public_school",
                "14 avenue",
              ],
            "compare_as": "TEXT"
          }
      ],
      "evaluation": "Exp1"
    }

### Available Operators

- FUZZ_MIN_SCORE_10/20/30, etc.

  "#;

        let exp_json = get_edited_user_json_input((&syntax).to_string());

        if handle_cancel_flag(&exp_json) {
            return Err("Operation canceled".into());
        }

        //dbg!(&exp_json);

        // Assume `last_exp_json` is a String containing your JSON data
        let parsed_json: Value = serde_json::from_str(&exp_json)?;

        let expressions = parsed_json["expressions"]
            .as_array()
            .ok_or("Invalid format for expressions")?;
        let mut expression_names = Vec::new();

        for (i, exp) in expressions.iter().enumerate() {
            let column = exp
                .get(1)
                .and_then(|col| col["column"].as_str())
                .ok_or("Invalid or missing column")?
                .to_string();

            let operator = exp
                .get(1)
                .and_then(|op| op["operator"].as_str())
                .ok_or("Invalid or missing operator")?
                .to_string();

            let compare_value = if let Some(compare_with_array) =
                exp.get(1).and_then(|cw| cw["compare_with"].as_array())
            {
                CompareValue::Multiple(
                    compare_with_array
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<String>>(), // Collecting as Vec<String>
                )
            } else if let Some(compare_with_single) =
                exp.get(1).and_then(|cw| cw["compare_with"].as_str())
            {
                CompareValue::Single(compare_with_single.to_string())
            } else {
                return Err("Invalid or missing compare_with".into());
            };

            let compare_type = if operator.starts_with("FUZZ_MIN_SCORE_") {
                "TEXT".to_string()
            } else {
                exp.get(1)
                    .and_then(|ct| ct["compare_as"].as_str())
                    .ok_or("Invalid or missing compare_as")?
                    .to_string()
            };

            // Add expressions to data store
            data_store.add_expression(column, operator, compare_value, compare_type);
            expression_names.push((format!("Exp{}", i + 1), i));
        }

        let result_expression = parsed_json["evaluation"]
            .as_str()
            .ok_or("Invalid or missing evaluation expression")?
            .to_string();

        Ok((expression_names, result_expression))
    }

    fn apply_filter_changes_menu(
        csv_builder: &mut CsvBuilder,
        prev_iteration_builder: &CsvBuilder,
        original_csv_builder: &CsvBuilder,
    ) -> Result<(), String> {
        let menu_options = vec![
            "Continue with tinkered data",
            "Discard this result, and load previous state",
            "Load original, to search from scratch",
        ];
        print_insight_level_2("Apply changes?");
        print_list_level_2(&menu_options);

        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        let selected_option = determine_action_as_number(&menu_options, &choice);

        match selected_option {
            Some(1) => {
                print_insight_level_2("Continuing with tinkered data");
                csv_builder.print_table();
                // Implement the logic for continuing with filtered data
                Ok(())
            }
            Some(2) => {
                print_insight_level_2("Discarding this result, and loading previous state");
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
            _ => Err("Invalid option. Please enter a number from 1 to 3.".to_string()),
        }
    }

    fn apply_limit(csv_builder: &mut CsvBuilder) -> Result<&mut CsvBuilder, String> {
        let syntax = r#"{
  "limit_value": "",
  "limit_type": "",
  "column_name_for_column_distribution": ""
}

SYNTAX
======

### Example 1

{
  "limit_value": "7",
  "limit_type": "NORMAL", // Also, "RANDOM", "RAW_DISTRIBUTION", "COLUMN_DISTRIBUTION"
  "column_name_for_column_distribution": "" // Leave empty if not applicable
}

### Example 2

{
  "limit_value": "7",
  "limit_type": "COLUMN_DISTRIBUTION", // Also, "RANDOM", "RAW_DISTRIBUTION", "COLUMN_DISTRIBUTION"
  "column_name_for_column_distribution": "Column7"
}

Note the implications of the limit_type value:
1. NORMAL: Directly restricts the dataset to the first 'n' entries, where 'n' is the specified limit, without considering distribution.
2. RANDOM: Selects 'n' random entries from the dataset, providing a sample that does not necessarily reflect the original distribution but ensures unpredictability.
3. RAW_DISTRIBUTION: Selects a representative sample from a larger dataset in a way that the selected sample mirrors the overall structure and distribution of the original data.
4. COLUMN_DISTRIBUTION: Balances the sample based on the distribution of values within a specified column, aiming to maintain proportional representation across different categories or values.
"#;

        let exp_json = get_edited_user_json_input((&syntax).to_string());

        if handle_cancel_flag(&exp_json) {
            return Err("Operation canceled".into());
        }

        //dbg!(&exp_json);

        //let parsed_json: Value = serde_json::from_str(&exp_json)?;

        let parsed_json: Value = serde_json::from_str(&exp_json).map_err(|e| e.to_string())?; // Convert the serde_json::Error into a String

        //dbg!(&parsed_json);

        let limit_value = parsed_json["limit_value"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let limit_type = parsed_json["limit_type"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let column_name_for_column_distribution = parsed_json
            ["column_name_for_column_distribution"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let limit = match limit_value.parse::<usize>() {
            Ok(num) => num,
            Err(_) => return Err("Invalid limit value".to_string()),
        };

        //dbg!(&limit_value, &limit_type, &column_name_for_column_distribution);

        match limit_type.as_str() {
            "NORMAL" => {
                csv_builder.limit(limit);
            }
            "RAW_DISTRIBUTION" => {
                csv_builder.limit_distributed_raw(limit);
            }
            "COLUMN_DISTRIBUTION" => {
                if column_name_for_column_distribution.is_empty() {
                    return Err(
                        "Column name for column distribution is required but was empty".to_string(),
                    );
                }
                csv_builder.limit_distributed_category(limit, &column_name_for_column_distribution);
            }
            "RANDOM" => {
                csv_builder.limit_random(limit);
            }
            _ => {
                return Err("Unsupported limit type".to_string());
            }
        }

        Ok(csv_builder)
    }

    let menu_options = vec![
        "SET HEADERS",
        "UPDATE HEADERS",
        "ADD ROWS",
        "UPDATE ROW",
        "EDIT TABLE (ASC)",
        "EDIT TABLE (DESC)",
        "DELETE ROWS",
        "FILTER ROWS",
        "LIMIT ROWS",
        "ADD COLUMNS",
        "DROP COLUMNS",
        "RETAIN COLUMNS",
        "REORDER COLUMNS",
        "SET INDEX COLUMN",
        "CASCADE SORT",
        "CLEAN DATA BY COLUMN PARSE",
    ];

    let original_csv_builder = CsvBuilder::from_copy(csv_builder);
    let choice = doc_request_flag;
    /*
        loop {
            print_insight_level_2("Select an option to search CSV data: ");
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
    */
    match action {
        "1" => {
            if choice.to_lowercase() == "1d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Sets header values for an empty csv.
{
  "headers": ["id", "item", "value"]
}

|id |item |value |
------------------
Total rows: 0
"#,
                );
                //Ok(())
                return Ok(());
                //continue;
            }

            let headers_json = json!({
                "headers": Vec::<String>::new()
            });

            let headers_json_str = match serde_json::to_string_pretty(&headers_json) {
                Ok(json) => json,
                Err(e) => {
                    eprintln!("Error creating JSON string: {}", e);
                    //return;
                    return Err("An error occurred".to_string().into());
                }
            };

            let edited_json = get_edited_user_json_input(headers_json_str);

            if handle_cancel_flag(&edited_json) {
                //Ok(())
                return Ok(());
                //continue;
            }

            let edited_headers: serde_json::Value = match serde_json::from_str(&edited_json) {
                Ok(headers) => headers,
                Err(e) => {
                    eprintln!("Error parsing JSON string: {}", e);
                    //return;
                    return Err("An error occurred".to_string().into());
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
                    //return;
                    return Err("An error occurred".to_string().into());
                }
            };

            let header_slices: Vec<&str> = headers.iter().map(AsRef::as_ref).collect();
            csv_builder.set_header(header_slices);

            if csv_builder.has_data() {
                csv_builder.print_table();
                println!();
            }

            /*
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

        "2" => {
            if choice.to_lowercase() == "2d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Updates header values.
{
  "existing_headers": [
    "id",
    "item",
    "value",
    "type",
    "item_type"
  ],
  "new_headers": [
    "id",
    "item",
    "value",
    "type",
    "category"
  ]
}


|id |item    |value |type  |category      |
-------------------------------------------
|1  |books   |1000  |small |books_small   |
|2  |snacks  |200   |small |snacks_small  |
|3  |cab fare|300   |small |cab fare_small|
|4  |rent    |20000 |big   |rent_big      |
|5  |movies  |1500  |medium|movies_medium |
Total rows: 5
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            let existing_headers = csv_builder.get_headers().unwrap_or(&[]).to_vec();

            let headers_json = json!({
                "existing_headers": existing_headers,
                "new_headers": Vec::<String>::new()
            });

            let headers_json_str = match serde_json::to_string_pretty(&headers_json) {
                Ok(json) => json,
                Err(e) => {
                    eprintln!("Error creating JSON string: {}", e);
                    //return;
                    return Err("An error occurred".to_string().into());
                }
            };

            let edited_json = get_edited_user_json_input(headers_json_str);
            //dbg!(&edited_json);
            if handle_cancel_flag(&edited_json) {
                //continue;
                //Ok(())
                return Ok(());
            }

            let edited_headers: serde_json::Value = match serde_json::from_str(&edited_json) {
                Ok(headers) => headers,
                Err(e) => {
                    eprintln!("Error parsing JSON string: {}", e);
                    //return;
                    return Err("An error occurred".to_string().into());
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
                    //return;
                    return Err("An error occurred".to_string().into());
                }
            };

            // Ensure new headers list is the same length as existing headers
            let max_length = csv_builder.get_headers().map_or(0, |headers| headers.len());
            let mut updated_headers = new_headers;
            updated_headers.resize(max_length, String::new());

            let header_slices: Vec<&str> = updated_headers.iter().map(AsRef::as_ref).collect();
            csv_builder.set_header(header_slices);

            if csv_builder.has_data() {
                csv_builder.print_table();
                println!();
            }

            /*
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

        "3" => {
            if choice.to_lowercase() == "3d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Adds rows.
[
  {
    "item": "tennis ball",
    "value": "400",
    "type": "small",
    "item_type": "tennis ball_small"
  }
]

|id |item       |value |type  |item_type        |
-------------------------------------------------
|1  |books      |1000  |small |books_small      |
|2  |snacks     |200   |small |snacks_small     |
|3  |cab fare   |300   |small |cab fare_small   |
|4  |rent       |20000 |big   |rent_big         |
|5  |movies     |1500  |medium|movies_medium    |
|6  |tennis ball|400   |small |tennis ball_small|
Total rows: 6
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            if let Some(headers) = csv_builder.get_headers() {
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
                //dbg!(&rows_json_str);
                if handle_cancel_flag(&rows_json_str) {
                    //continue;
                    //Ok(())
                    return Ok(());
                }

                // Parse the user input
                let rows_json: Vec<serde_json::Value> = match serde_json::from_str(&rows_json_str) {
                    Ok(json) => json,
                    Err(e) => {
                        eprintln!("Error parsing JSON string: {}", e);
                        //return; // Exit the function early if there's an error
                        return Err("An error occurred".to_string().into());
                    }
                };

                let mut all_rows = Vec::new();

                // Logic to find the current maximum ID
                let mut next_id = csv_builder
                    .get_data()
                    .unwrap()
                    .iter()
                    .filter_map(|row| row.get(headers.iter().position(|h| h == "id").unwrap_or(0)))
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

                csv_builder.add_rows(rows_as_str_slices);
                csv_builder.print_table();
                println!();
                //continue;
            } else {
                print_insight_level_2("No headers set. Cannot add rows.");
            }

            /*
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

        "4" => {
            if choice.to_lowercase() == "4d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Updates a row.

|id |item       |value |type  |item_type        |
-------------------------------------------------
|1  |books      |1000  |small |books_small      |
|2  |snacks     |200   |small |snacks_small     |
|3  |cab fare   |300   |small |cab fare_small   |
|4  |rent       |20000 |big   |rent_big         |
|5  |movies     |1500  |medium|movies_medium    |
|6  |tennis ball|400   |small |tennis ball_small|
Total rows: 6

  @LILbro: Enter the id of the row to update: 3

{
  "item": "cab fare",
  "value": "500",
  "type": "small",
  "item_type": "cab fare_small"
}

|id |item       |value |type  |item_type        |
-------------------------------------------------
|1  |books      |1000  |small |books_small      |
|2  |snacks     |200   |small |snacks_small     |
|3  |cab fare   |500   |small |cab fare_small   |
|4  |rent       |20000 |big   |rent_big         |
|5  |movies     |1500  |medium|movies_medium    |
|6  |tennis ball|400   |small |tennis ball_small|
Total rows: 6
"#,
                );
                //Ok(())
                return Ok(());
                //continue;
            }

            if !csv_builder.has_data() {
                eprintln!("No data available to update.");
                //return;
                return Err("An error occurred".to_string().into());
            }

            // Display existing data
            csv_builder.print_table();
            println!();

            let use_id_for_update = csv_builder
                .get_headers()
                .map_or(false, |headers| headers.contains(&"id".to_string()));
            let zero_based_index: usize;
            let mut original_id = String::new();

            if use_id_for_update {
                let id_str = get_user_input_level_2("Enter the id of the row to update: ");
                let id = id_str.trim();

                if let Some((index, _)) = csv_builder
                    .get_data()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .find(|(_, row)| row.get(0) == Some(&id.to_string()))
                {
                    zero_based_index = index;
                    original_id = id.to_string();
                } else {
                    eprintln!("ID not found.");
                    //return;
                    return Err("An error occurred".to_string().into());
                }
            } else {
                let row_index_str =
                    get_user_input_level_2("Enter the index of the row to update: ");
                let row_index: usize = match row_index_str.trim().parse() {
                    Ok(num) => num,
                    Err(_) => {
                        eprintln!("Invalid input for row index.");
                        //return;
                        return Err("An error occurred".to_string().into());
                    }
                };

                zero_based_index = row_index.saturating_sub(1);
            }

            if zero_based_index >= csv_builder.get_data().unwrap().len() {
                eprintln!("Row index out of range.");
                //return;
                return Err("An error occurred".to_string().into());
            }

            if let Some(existing_row) = csv_builder.get_data().unwrap().get(zero_based_index) {
                if let Some(headers) = csv_builder.get_headers() {
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

                    if handle_cancel_flag(&edited_json) {
                        //continue;
                        //Ok(())
                        return Ok(());
                    }

                    let edited_row: serde_json::Value = match serde_json::from_str(&edited_json) {
                        Ok(row) => row,
                        Err(e) => {
                            eprintln!("Error parsing JSON string: {}", e);
                            //return;
                            return Err("An error occurred".to_string().into());
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
                                    _ => {
                                        edited_row[header].as_str().unwrap_or_default().to_string()
                                    }
                                }
                            }
                        })
                        .collect::<Vec<String>>();
                    csv_builder.update_row_by_row_number(
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

            csv_builder.print_table();
            println!();

            /*
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
        "5" => {
            if choice.to_lowercase() == "5d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to edit a table (header as well as row values) in vim.

|id |item    |value |type |item_type     |
------------------------------------------
|1  |books   |1000  |small|books_small   |
|2  |snacks  |200   |small|snacks_small  |
|3  |cab fare|300   |small|cab fare_small|
Total rows: 3

  @LILbro: Executing this JSON query:
[
  {
    "x_id": "1",
    "item": "books",
    "value": "1000",
    "type": "small",
    "item_type": "books_small"
  },
  {
    "x_id": "2",
    "item": "snacks",
    "value": "500",
    "type": "small",
    "item_type": "snacks_small"
  },
  {
    "x_id": "3",
    "item": "cab fare",
    "value": "300",
    "type": "small",
    "item_type": "cab fare_small"
  }
]

|x_id |item    |value |type |item_type     |
--------------------------------------------
|1    |books   |1000  |small|books_small   |
|2    |snacks  |500   |small|snacks_small  |
|3    |cab fare|300   |small|cab fare_small|
Total rows: 3
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            let existing_data_option = csv_builder.get_data();

            let existing_headers: Vec<String> = csv_builder
                .get_headers()
                .unwrap_or(&[])
                .iter()
                .cloned() // Clone each String in the Vec
                .collect();

            let mut json_array_str = "[".to_string();

            if existing_data_option == None {
                // Handle case when there is no data
                json_array_str.push_str("\n  {");
                /*
                for (col_index, header) in existing_headers.iter().enumerate() {
                    json_array_str.push_str(&format!("\n    \"{}\": \"\"", header));
                    if col_index < existing_headers.len() - 1 {
                        json_array_str.push(',');
                    }
                }
                */
                for (col_index, header) in existing_headers.iter().enumerate() {
                    json_array_str.push_str(&format!("\n    \"{}\": \"\"", header));
                    if col_index < existing_headers.len() - 1 {
                        json_array_str.push(',');
                    }
                }

                json_array_str.push_str("\n  }");
            } else {
                let existing_data = existing_data_option.unwrap();
                // Original logic for when data exists
                for (row_index, row) in existing_data.iter().enumerate() {
                    json_array_str.push_str("\n  {");

                    for (col_index, value) in row.iter().enumerate() {
                        json_array_str.push_str(&format!(
                            "\n    \"{}\": \"{}\"",
                            existing_headers[col_index], value
                        ));
                        if col_index < existing_headers.len() - 1 {
                            json_array_str.push(',');
                        }
                    }

                    json_array_str.push_str("\n  }");
                    if row_index < existing_data.len() - 1 {
                        json_array_str.push(',');
                    }
                }
            }
            json_array_str.push_str("\n]");

            let syntax_explanation = r#"

SYNTAX
======

### Example

[
    {
        "new_column1": "value1",
        "new_column2": "value2",
        // ...
    },
    {
        "new_column1": "value1",
        "new_column2": "value2",
        // ...
    }
    // ...
]

    "#;
            let full_syntax = json_array_str + syntax_explanation;

            let rows_json_str = get_edited_user_json_input(full_syntax);

            if handle_cancel_flag(&rows_json_str) {
                //continue;
                //Ok(())
                return Ok(());
            }

            // Parse the Edited JSON String
            let rows_json: Vec<Value> = serde_json::from_str(&rows_json_str)?;

            if rows_json.is_empty() {
                return Err("No data provided".into());
            }

            // Collect all unique keys from all objects to ensure none are missed
            let mut all_keys: HashSet<String> = HashSet::new();
            for obj in rows_json.iter().filter_map(|v| v.as_object()) {
                for key in obj.keys() {
                    all_keys.insert(key.clone());
                }
            }

            // Sort keys by their first occurrence in the JSON string
            let mut keys_with_positions: Vec<(String, usize)> = all_keys
                .into_iter()
                .filter_map(|key| {
                    let search_key = format!("\"{}\":", key);
                    rows_json_str.find(&search_key).map(|pos| (key, pos))
                })
                .collect();

            keys_with_positions.sort_by_key(|k| k.1);
            let headers: Vec<String> = keys_with_positions.into_iter().map(|k| k.0).collect();

            let data: Result<Vec<Vec<String>>, Box<dyn Error>> = rows_json
                .into_iter()
                .map(|row| {
                    if let Value::Object(obj) = row {
                        let row_data: Vec<String> = headers
                            .iter()
                            .map(|header| {
                                obj.get(header)
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string()
                            })
                            .collect();
                        Ok(row_data)
                    } else {
                        Err("Row is not an object".into())
                    }
                })
                .collect();

            let data = data?;

            // Create a new CsvBuilder from the parsed JSON
            let new_csv_builder = CsvBuilder::from_raw_data(headers, data);

            // Override the existing CsvBuilder with the new one
            csv_builder.override_with(&new_csv_builder);

            csv_builder.print_table();

            println!();
            /*
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

        "6" => {
            if choice.to_lowercase() == "6d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to edit a table (header as well as row values) in vim, in reverse order.

|id |item    |value |type |item_type     |
------------------------------------------
|1  |books   |1000  |small|books_small   |
|2  |snacks  |200   |small|snacks_small  |
|3  |cab fare|300   |small|cab fare_small|
Total rows: 3

  @LILbro: Executing this JSON query:
[
  {
    "x_id": "3",
    "item": "cab fare",
    "value": "300",
    "type": "small",
    "item_type": "cab fare_small"
  },
  {
    "x_id": "2",
    "item": "snacks",
    "value": "500",
    "type": "small",
    "item_type": "snacks_small"
  },
  {
    "x_id": "1",
    "item": "books",
    "value": "1000",
    "type": "small",
    "item_type": "books_small"
  }
]

|x_id |item    |value |type |item_type     |
--------------------------------------------
|1    |books   |1000  |small|books_small   |
|2    |snacks  |500   |small|snacks_small  |
|3    |cab fare|300   |small|cab fare_small|
Total rows: 3
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            let existing_data_option = csv_builder.get_data();

            let existing_headers: Vec<String> = csv_builder
                .get_headers()
                .unwrap_or(&[])
                .iter()
                .cloned() // Clone each String in the Vec
                .collect();
            let mut json_array_str = "[".to_string();

            if existing_data_option == None {
                // Handle case when there is no data: add one JSON object with all headers and empty strings
                json_array_str.push_str("\n  {");
                for (col_index, header) in existing_headers.iter().enumerate() {
                    json_array_str.push_str(&format!("\n    \"{}\": \"\"", header));
                    if col_index < existing_headers.len() - 1 {
                        json_array_str.push(',');
                    }
                }
                json_array_str.push_str("\n  }");
            } else {
                let existing_data = existing_data_option.unwrap();
                // If there is data, iterate in reverse and construct JSON string
                for row in existing_data.iter().rev() {
                    json_array_str.push_str("\n  {");

                    for (col_index, value) in row.iter().enumerate() {
                        json_array_str.push_str(&format!(
                            "\n    \"{}\": \"{}\"",
                            existing_headers[col_index], value
                        ));
                        if col_index < row.len() - 1 {
                            json_array_str.push(',');
                        }
                    }

                    json_array_str.push_str("\n  }");
                    if json_array_str.ends_with("}") && !json_array_str.ends_with("]") {
                        json_array_str.push(',');
                    }
                }
                if json_array_str.ends_with(",") {
                    json_array_str.pop(); // Remove the last comma if it exists
                }
            }
            json_array_str.push_str("\n]");

            let syntax_explanation = r#"

SYNTAX
======

### Example

[
    {
        "new_column1": "value1",
        "new_column2": "value2",
        // ...
    },
    {
        "new_column1": "value1",
        "new_column2": "value2",
        // ...
    }
    // ...
]

    "#;
            let full_syntax = json_array_str + syntax_explanation;

            let rows_json_str = get_edited_user_json_input(full_syntax);

            if handle_cancel_flag(&rows_json_str) {
                //continue;
                //Ok(())
                return Ok(());
            }

            // Parse the Edited JSON String
            let mut rows_json: Vec<Value> = serde_json::from_str(&rows_json_str)?;
            rows_json.reverse();

            if rows_json.is_empty() {
                return Err("No data provided".into());
            }

            // Collect all unique keys from all objects to ensure none are missed
            let mut all_keys: HashSet<String> = HashSet::new();
            for obj in rows_json.iter().filter_map(|v| v.as_object()) {
                for key in obj.keys() {
                    all_keys.insert(key.clone());
                }
            }

            // Sort keys by their first occurrence in the JSON string
            let mut keys_with_positions: Vec<(String, usize)> = all_keys
                .into_iter()
                .filter_map(|key| {
                    let search_key = format!("\"{}\":", key);
                    rows_json_str.find(&search_key).map(|pos| (key, pos))
                })
                .collect();

            keys_with_positions.sort_by_key(|k| k.1);
            let headers: Vec<String> = keys_with_positions.into_iter().map(|k| k.0).collect();

            let data: Result<Vec<Vec<String>>, Box<dyn Error>> = rows_json
                .into_iter()
                .map(|row| {
                    if let Value::Object(obj) = row {
                        let row_data: Vec<String> = headers
                            .iter()
                            .map(|header| {
                                obj.get(header)
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string()
                            })
                            .collect();
                        Ok(row_data)
                    } else {
                        Err("Row is not an object".into())
                    }
                })
                .collect();

            let data = data?;

            // Create a new CsvBuilder from the parsed JSON
            let new_csv_builder = CsvBuilder::from_raw_data(headers, data);

            // Override the existing CsvBuilder with the new one
            csv_builder.override_with(&new_csv_builder);

            csv_builder.print_table();
            println!();
            /*
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

        "7" => {
            if choice.to_lowercase() == "7d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to delete rows.
|id |item    |value |type  |item_type     |
-------------------------------------------
|1  |books   |1000  |small |books_small   |
|2  |snacks  |200   |small |snacks_small  |
|3  |cab fare|300   |small |cab fare_small|
|4  |rent    |20000 |big   |rent_big      |
|5  |movies  |1500  |medium|movies_medium |
Total rows: 5

  @LILbro: Enter the identifiers (ID or indices) of the rows to delete (comma-separated), or type 'back' to return: 4, 5
2 row(s) deleted successfully.

|id |item    |value |type |item_type     |
------------------------------------------
|1  |books   |1000  |small|books_small   |
|2  |snacks  |200   |small|snacks_small  |
|3  |cab fare|300   |small|cab fare_small|
Total rows: 3
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            if !csv_builder.has_data() {
                eprintln!("No data available for deletion.");
                //return;
                return Err("An error occurred".to_string().into());
            }

            // Display existing data
            csv_builder.print_table();
            println!();

            let use_id_for_deletion = csv_builder
                .get_headers()
                .map_or(false, |headers| headers.contains(&"id".to_string()));

            let row_identifiers_str = get_user_input_level_2(
                "Enter the identifiers (ID or indices) of the rows to delete (comma-separated): ",
            );

            if handle_cancel_flag(&row_identifiers_str) {
                //continue;
                //Ok(())
                return Ok(());
            }

            let mut deleted_count = 0;

            if use_id_for_deletion {
                // Parse as IDs
                let ids: Vec<&str> = row_identifiers_str.split(',').map(|s| s.trim()).collect();

                for id in ids {
                    if csv_builder.delete_row_by_id(id) {
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
                    //return;
                    return Err("An error occurred".to_string().into());
                }

                // Sort indices in descending order to avoid index shift during deletion
                let mut sorted_indices = row_indices;
                sorted_indices.sort_by(|a, b| b.cmp(a));

                for index in sorted_indices {
                    if csv_builder.delete_row_by_row_number(index) {
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
            csv_builder.print_table();
            println!();

            /*
            match apply_filter_changes_menu(
                csv_builder,
                &prev_iteration_builder,
                &original_csv_builder,
            ) {
                Ok(_) => (),
                Err(e) => {
                    println!("{}", e);
                    continue;
                }
            }
            */
        }

        "8" => {
            if choice.to_lowercase() == "8d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to filter rows.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|6  |books   |1000  |OTHER |2024-03-21|0                 |Y2024-M03       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
Total rows: 10

  @LILbro: Executing this JSON query:
{

  "expressions": [
    [
      "Exp1",
      {
        "column": "type",
        "operator": "==",
        "compare_with": "FOOD",
        "compare_as": "TEXT"
      }
    ],
    [
      "Exp2",
      {
        "column": "value",
        "operator": "<",
        "compare_with": "500",
        "compare_as": "NUMBERS"
      }
    ]
  ],
  "evaluation": "Exp1 || Exp2"
}

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
Total rows: 4
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            if !csv_builder.has_data() {
                eprintln!("No data available for deletion.");
                //return;
                return Err("An error occurred".to_string().into());
            }

            let mut exp_store = ExpStore {
                expressions: Vec::new(),
            };

            match get_filter_expressions(&mut exp_store) {
                Ok((expression_names, result_expression)) => {
                    let expressions_refs: Vec<(&str, Exp)> = expression_names
                        .iter()
                        .map(|(name, index)| (name.as_str(), exp_store.get_exp(*index).clone()))
                        .collect();

                    //dbg!(&expressions_refs, &result_expression);
                    csv_builder.where_(expressions_refs, &result_expression);

                    csv_builder.print_table();
                    println!();
                    /*
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
                Err(e) if e.to_string() == "Operation canceled" => {
                    //Ok(())
                    return Ok(());
                    //continue;
                }
                Err(e) => {
                    println!("Error getting filter expressions: {}", e);
                    //Ok(())
                    return Ok(());
                    //continue; // Return to the menu to let the user try again or choose another option
                }
            }
        }

        "9" => {
            if choice.to_lowercase() == "9d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to limit rows.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|6  |books   |1000  |OTHER |2024-03-21|0                 |Y2024-M03       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
Total rows: 10

Example 1: NORMAL
{
  "limit_value": "5",
  "limit_type": "NORMAL",
  "column_name_for_column_distribution": ""
}

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
Total rows: 5

Example 2: RANDOM
{
  "limit_value": "5",
  "limit_type": "RANDOM",
  "column_name_for_column_distribution": ""
}

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
Total rows: 5

Example 3: RAW_DISTRIBUTION
{
  "limit_value": "5",
  "limit_type": "RAW_DISTRIBUTION",
  "column_name_for_column_distribution": ""
}

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
Total rows: 5

Example 4: COLUMN_DISTRIBUTION
{
  "limit_value": "5",
  "limit_type": "COLUMN_DISTRIBUTION",
  "column_name_for_column_distribution": "type"
}

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
Total rows: 5

Note the implications of the limit_type value:
1. NORMAL: Directly restricts the dataset to the first 'n' entries, where 'n' is the specified limit, without considering distribution.
2. RANDOM: Selects 'n' random entries from the dataset, providing a sample that does not necessarily reflect the original distribution but ensures unpredictability.
3. RAW_DISTRIBUTION: Selects a representative sample from a larger dataset in a way that the selected sample mirrors the overall structure and distribution of the original data.
4. COLUMN_DISTRIBUTION: Balances the sample based on the distribution of values within a specified column, aiming to maintain proportional representation across different categories or values.
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            if !csv_builder.has_data() {
                eprintln!("No data available for deletion.");
                //return;
                return Err("An error occurred".to_string().into());
            }

            match apply_limit(csv_builder) {
                Ok(csv_builder) => {
                    csv_builder.print_table();
                    println!();
                    /*
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
                Err(e) if e.to_string() == "Operation canceled" => {
                    // If the operation was canceled by the user, do not print an error and just continue
                    //continue;
                    //Ok(())
                    return Ok(());
                }
                Err(e) => {
                    println!("Error getting limit expressions: {}", e);
                    //continue; // Return to the menu to let the user try again or choose another option
                    //Ok(())
                    return Ok(());
                }
            }
        }

        "10" => {
            if choice.to_lowercase() == "10d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to add column headers, and set their values.
|id |item    |value |type |item_type     |
------------------------------------------
|1  |books   |1000  |small|books_small   |
|2  |snacks  |200   |small|snacks_small  |
|3  |cab fare|300   |small|cab fare_small|
Total rows: 3

  @LILbro: Executing this JSON query:
[
  {
    "id": "1",
    "item": "books",
    "value": "1000",
    "type": "small",
    "item_type": "books_small",
    "paid_via_cc": "yes"
  },
  {
    "id": "2",
    "item": "snacks",
    "value": "200",
    "type": "small",
    "item_type": "snacks_small",
    "paid_via_cc": "yes"
  },
  {
    "id": "3",
    "item": "cab fare",
    "value": "300",
    "type": "small",
    "item_type": "cab fare_small",
    "paid_via_cc": "no"
  }
]

|id |item    |value |type |item_type     |paid_via_cc |
-------------------------------------------------------
|1  |books   |1000  |small|books_small   |yes         |
|2  |snacks  |200   |small|snacks_small  |yes         |
|3  |cab fare|300   |small|cab fare_small|no          |
Total rows: 3
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            let new_columns_input = get_user_input_level_2("Enter new column names: ");

            if handle_cancel_flag(&new_columns_input) {
                //continue;
                //Ok(())
                return Ok(());
            }

            println!();
            //let new_columns: Vec<&str> = new_columns_input.trim().split(',').collect();

            let new_columns: Vec<String> = new_columns_input
                .trim() // Trim the whole input to remove leading/trailing whitespace around the input
                .split(',') // Split the input into individual column names
                .map(|name| name.trim().to_string()) // Trim each column name and convert to String
                .collect();

            if !new_columns.is_empty() {
                let existing_data = csv_builder.get_data().unwrap();
                let existing_headers = csv_builder.get_headers().unwrap_or(&[]); // Assuming get_headers() method exists

                let mut json_array_str = "[".to_string();

                for (row_index, row) in existing_data.iter().enumerate() {
                    json_array_str.push_str("\n  {");

                    // Include existing row values
                    for (col_index, value) in row.iter().enumerate() {
                        json_array_str.push_str(&format!(
                            "\n    \"{}\": \"{}\"",
                            existing_headers[col_index], value
                        ));
                        if col_index < existing_headers.len() - 1 || !new_columns.is_empty() {
                            json_array_str.push(',');
                        }
                    }

                    // Add placeholders for new columns
                    for (new_col_index, new_col) in new_columns.iter().enumerate() {
                        json_array_str.push_str(&format!("\n    \"{}\": \"\"", new_col));
                        if new_col_index < new_columns.len() - 1 {
                            json_array_str.push(',');
                        }
                    }

                    json_array_str.push_str("\n  }");
                    if row_index < existing_data.len() - 1 {
                        json_array_str.push(',');
                    }
                }

                json_array_str.push_str("\n]");

                let syntax_explanation = r#"

SYNTAX
======

### Example

[
    {
        "new_column1": "value1",
        "new_column2": "value2",
        // ...
    },
    {
        "new_column1": "value1",
        "new_column2": "value2",
        // ...
    }
    // ...
]

    "#;
                let full_syntax = json_array_str + syntax_explanation;

                let rows_json_str = get_edited_user_json_input(full_syntax);

                if handle_cancel_flag(&rows_json_str) {
                    //continue;
                    //Ok(())
                    return Ok(());
                }

                let rows_json: Vec<serde_json::Value> = match serde_json::from_str(&rows_json_str) {
                    Ok(json) => json,
                    Err(e) => {
                        eprintln!("Error parsing JSON string: {}", e);
                        //return; // Exit the function early if there's an error
                        return Err("An error occurred".to_string().into());
                    }
                };
                let new_columns_str_slices: Vec<&str> =
                    new_columns.iter().map(AsRef::as_ref).collect();

                // Now call add_column_headers with the corrected type
                csv_builder.add_column_headers(new_columns_str_slices);

                // Add new column headers
                //builder.add_column_headers(new_columns.clone());

                for (row_index, row_json) in rows_json.iter().enumerate() {
                    // Collect new and existing values for the current row
                    let mut row_values = Vec::new();
                    if let Some(headers) = csv_builder.get_headers() {
                        for header in headers {
                            if let Some(value) = row_json.get(header).and_then(|v| v.as_str()) {
                                row_values.push(value);
                            } else {
                                // For new columns or missing values, default to an empty string
                                row_values.push("");
                            }
                        }
                    }

                    // Update existing rows or add new ones
                    if row_index < csv_builder.get_data().unwrap().len() {
                        // Update existing row with new and existing values
                        csv_builder.update_row_by_row_number(row_index + 1, row_values.clone());
                    } else {
                        // Add new rows with the provided values
                        csv_builder.add_row(row_values);
                    }
                }

                csv_builder.print_table();
            } else {
                println!("No columns entered. Exiting ADD COLUMNS function.");
            }

            /*
            match apply_filter_changes_menu(
                csv_builder,
                &prev_iteration_builder,
                &original_csv_builder,
            ) {
                Ok(_) => (),
                Err(e) => {
                    println!("{}", e);
                    continue;
                }
            }
            */
        }

        "11" => {
            if choice.to_lowercase() == "11d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Drop specific columns.
|id |item    |value |type |item_type     |paid_via_cc |
-------------------------------------------------------
|1  |books   |1000  |small|books_small   |yes         |
|2  |snacks  |200   |small|snacks_small  |yes         |
|3  |cab fare|300   |small|cab fare_small|no          |
Total rows: 3

  @LILbro: Please type a comma-separated list of columns: paid_via_cc, item_type

|id |item    |value |type |
---------------------------
|1  |books   |1000  |small|
|2  |snacks  |200   |small|
|3  |cab fare|300   |small|
Total rows: 3
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            let columns_input =
                get_user_input_level_2("Please type a comma-separated list of columns: ");

            if handle_cancel_flag(&columns_input) {
                //continue;
                //Ok(())
                return Ok(());
            }

            let columns: Vec<&str> = columns_input.trim().split(',').map(|s| s.trim()).collect();

            csv_builder.drop_columns(columns).print_table();

            /*
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
        "12" => {
            if choice.to_lowercase() == "12d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Retain specific columns, in a specific order.
|id |item    |value |type  |item_type     |
-------------------------------------------
|1  |books   |1000  |small |books_small   |
|2  |snacks  |200   |small |snacks_small  |
|3  |cab fare|300   |small |cab fare_small|
|4  |rent    |20000 |big   |rent_big      |
|5  |movies  |1500  |medium|movies_medium |
Total rows: 5

  @LILbro: Please type a comma-separated list of columns: id, item_type, value

|id |item_type     |value |
---------------------------
|1  |books_small   |1000  |
|2  |snacks_small  |200   |
|3  |cab fare_small|300   |
|4  |rent_big      |20000 |
|5  |movies_medium |1500  |
Total rows: 5
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            let columns_input =
                get_user_input_level_2("Please type a comma-separated list of columns: ");

            if handle_cancel_flag(&columns_input) {
                //continue;
                //Ok(())
                return Ok(());
            }

            let columns: Vec<&str> = columns_input.trim().split(',').map(|s| s.trim()).collect();

            csv_builder.retain_columns(columns).print_table();

            /*
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

        "13" => {
            if choice.to_lowercase() == "13d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to re-order columns, without the need to specify an exact order, and referencing the existing arrangement with '...', in the manner shown below.

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|6  |books   |1000  |OTHER |2024-03-21|0                 |Y2024-M03       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
Total rows: 10

  @LILbro: Specify new column order: type, date, ...

|type  |date      |id |item    |value |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|type  |date      |id |item    |value |relates_to_travel |date_YEAR_MONTH |
|OTHER |2024-01-21|1  |books   |1000  |0                 |Y2024-M01       |
|FOOD  |2024-02-22|2  |snacks  |200   |0                 |Y2024-M02       |
|TRAVEL|2024-03-23|3  |cab fare|300   |1                 |Y2024-M03       |
|OTHER |2024-01-24|4  |rent    |20000 |0                 |Y2024-M01       |
<<+1 row>>
|OTHER |2024-03-21|6  |books   |1000  |0                 |Y2024-M03       |
|FOOD  |2024-01-22|7  |snacks  |200   |0                 |Y2024-M01       |
|TRAVEL|2024-02-23|8  |cab fare|300   |1                 |Y2024-M02       |
|OTHER |2024-03-24|9  |rent    |20000 |0                 |Y2024-M03       |
|OTHER |2024-01-25|10 |movies  |1500  |0                 |Y2024-M01       |
Total rows: 11

  @LILbro: Specify new column order: ..., date, type

|id |item    |value |relates_to_travel |date_YEAR_MONTH |date      |type  |
---------------------------------------------------------------------------
|id |item    |value |relates_to_travel |date_YEAR_MONTH |date      |type  |
|1  |books   |1000  |0                 |Y2024-M01       |2024-01-21|OTHER |
|2  |snacks  |200   |0                 |Y2024-M02       |2024-02-22|FOOD  |
|3  |cab fare|300   |1                 |Y2024-M03       |2024-03-23|TRAVEL|
|4  |rent    |20000 |0                 |Y2024-M01       |2024-01-24|OTHER |
<<+1 row>>
|6  |books   |1000  |0                 |Y2024-M03       |2024-03-21|OTHER |
|7  |snacks  |200   |0                 |Y2024-M01       |2024-01-22|FOOD  |
|8  |cab fare|300   |1                 |Y2024-M02       |2024-02-23|TRAVEL|
|9  |rent    |20000 |0                 |Y2024-M03       |2024-03-24|OTHER |
|10 |movies  |1500  |0                 |Y2024-M01       |2024-01-25|OTHER |
Total rows: 11

  @LILbro: Specify new column order: date, type, ..., id

|date      |type  |item    |value |relates_to_travel |date_YEAR_MONTH |id |
---------------------------------------------------------------------------
|date      |type  |item    |value |relates_to_travel |date_YEAR_MONTH |id |
|2024-01-21|OTHER |books   |1000  |0                 |Y2024-M01       |1  |
|2024-02-22|FOOD  |snacks  |200   |0                 |Y2024-M02       |2  |
|2024-03-23|TRAVEL|cab fare|300   |1                 |Y2024-M03       |3  |
|2024-01-24|OTHER |rent    |20000 |0                 |Y2024-M01       |4  |
<<+1 row>>
|2024-03-21|OTHER |books   |1000  |0                 |Y2024-M03       |6  |
|2024-01-22|FOOD  |snacks  |200   |0                 |Y2024-M01       |7  |
|2024-02-23|TRAVEL|cab fare|300   |1                 |Y2024-M02       |8  |
|2024-03-24|OTHER |rent    |20000 |0                 |Y2024-M03       |9  |
|2024-01-25|OTHER |movies  |1500  |0                 |Y2024-M01       |10 |
Total rows: 11
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            let new_columns_order_input =
                get_user_input_level_2("Specify new column order: ").to_lowercase();
            /*
            if new_columns_order_input.to_lowercase() == "@cancel" {
                continue;
            }
            */
            if handle_cancel_flag(&new_columns_order_input) {
                //continue;
                //Ok(())
                return Ok(());
            }

            let new_columns_order: Vec<&str> = new_columns_order_input
                .split(',')
                .map(|s| s.trim()) // Trim whitespace around the column names
                .collect();

            // To be updated
            csv_builder.order_columns(new_columns_order).print_table();

            /*
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

        "14" => {
            if choice.to_lowercase() == "14d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Sets the values of the specified column sequentially from 1 onwards, ensuring each entry is uniquely numbered in ascending order until the last row. If the column does not exist, it creates a new column.
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
<<+12 rows>>
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|11 |concert |2000  |OTHER |2024-03-27|0                 |Y2024-M03       |
|12 |alcohol |1100  |OTHER |2024-03-28|0                 |Y2024-M03       |
Total rows: 22

Example 1
+++++++++

  @LILbro: Name of id column: id

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|3  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|4  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|5  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
<<+12 rows>>
|18 |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|19 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|20 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|21 |concert |2000  |OTHER |2024-03-27|0                 |Y2024-M03       |
|22 |alcohol |1100  |OTHER |2024-03-28|0                 |Y2024-M03       |
Total rows: 22

Example 2
+++++++++

@LILbro: Name of id column: account_id

|account_id |id |item    |value |  <<+1 col>>   |date      |relates_to_travel |date_YEAR_MONTH |
------------------------------------------------------------------------------------------------
|1          |1  |books   |1000  |...            |2024-01-21|0                 |Y2024-M01       |
|2          |2  |snacks  |200   |...            |2024-02-22|0                 |Y2024-M02       |
|3          |3  |cab fare|300   |...            |2024-03-23|1                 |Y2024-M03       |
|4          |4  |rent    |20000 |...            |2024-01-24|0                 |Y2024-M01       |
|5          |5  |movies  |1500  |...            |2024-02-25|0                 |Y2024-M02       |
|6          |6  |books   |1000  |...            |2024-03-21|0                 |Y2024-M03       |
|7          |7  |snacks  |200   |...            |2024-01-22|0                 |Y2024-M01       |
|8          |8  |cab fare|300   |...            |2024-02-23|1                 |Y2024-M02       |
|9          |9  |rent    |20000 |...            |2024-03-24|0                 |Y2024-M03       |
|10         |10 |movies  |1500  |...            |2024-01-25|0                 |Y2024-M01       |

Omitted columns: type
Total rows: 10
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            let id_column_name = get_user_input_level_2("Name of id column: ").to_lowercase();
            /*
            if id_column_name.to_lowercase() == "@cancel" {
                continue;
            }
            */
            if handle_cancel_flag(&id_column_name) {
                //continue;
                //Ok(())
                return Ok(());
            }

            let mut add_new_column_header = false;

            if let Some(headers) = csv_builder.get_headers() {
                for header in headers.iter() {
                    if header != id_column_name.as_str() {
                        add_new_column_header = true;
                        break; // No need to continue once we've found an "id" header
                    }
                }
            }

            if add_new_column_header {
                let _ = csv_builder.add_column_header(&id_column_name);
            }

            //dbg!(&csv_builder);

            let _ = csv_builder.resequence_id_column(&id_column_name);

            //dbg!(&csv_builder);
            if add_new_column_header {
                let _ = csv_builder.cascade_sort(vec![(id_column_name.clone(), "ASC".to_string())]);
            }

            //dbg!(&csv_builder);

            csv_builder
                .order_columns(vec![&id_column_name, "..."])
                .print_table();

            //dbg!(&csv_builder);

            //    csv_builder.print_table();
            /*
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

        "15" => {
            if choice.to_lowercase() == "15d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to sort by columns, in a cascaded manner.

TABLE
+++++

|id |value |date      |interest |
---------------------------------
|1  |500   |2024-04-08|7        |
|2  |500   |2024-04-07|8        |
|3  |500   |2024-04-06|9        |
|4  |400   |2024-04-05|7        |
|5  |400   |2024-04-05|7.2      |
|6  |400   |2024-04-03|8.2      |
|7  |300   |2024-04-02|9.2      |
|8  |300   |2024-04-01|7.4      |
|9  |300   |2024-04-08|8.4      |
|10 |300   |2024-04-08|9.4      |
Total rows: 10

  @LILbro: Executing this JSON query:
{
    "sort_orders": [
        {"column": "value", "order": "ASC"},
	    {"column": "date", "order": "DESC"},
	    {"column": "interest", "order": "ASC"},
	    {"column": "id", "order": "DESC"}
    ]
}

|id |value |date      |interest |
---------------------------------
|9  |300   |2024-04-08|8.4      |
|10 |300   |2024-04-08|9.4      |
|7  |300   |2024-04-02|9.2      |
|8  |300   |2024-04-01|7.4      |
|4  |400   |2024-04-05|7        |
|5  |400   |2024-04-05|7.2      |
|6  |400   |2024-04-03|8.2      |
|1  |500   |2024-04-08|7        |
|2  |500   |2024-04-07|8        |
|3  |500   |2024-04-06|9        |
Total rows: 10
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

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

            if handle_cancel_flag(&sort_json) {
                //continue;
                //Ok(())
                return Ok(());
            }

            // Parse the user input
            let sort_orders = {
                let parsed_sort_orders = match serde_json::from_str::<serde_json::Value>(&sort_json)
                {
                    Ok(config) => config,
                    Err(e) => {
                        eprintln!("Error parsing JSON: {}", e);
                        //Ok(())
                        return Ok(());
                        //continue; // Exit the function early if there's an error
                    }
                };

                // Extract sort orders
                parsed_sort_orders["sort_orders"]
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .filter_map(|order| {
                        let column = order["column"].as_str()?.to_string(); // Convert directly to String
                        let order = order["order"].as_str()?.to_string(); // Convert directly to String
                        Some((column, order))
                    })
                    .collect::<Vec<(String, String)>>()
            };

            // Apply the cascade sort
            csv_builder.cascade_sort(sort_orders);

            if csv_builder.has_data() {
                csv_builder.print_table();
                println!();
            }

            //    csv_builder.print_table();
            /*
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

        "16" => {
            if choice.to_lowercase() == "16d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

// Cleans data by parsing columns with preset rules. Rows that do not conform to any of the stipulated rules are discarded
  @LILbro: Executing this JSON query:
{
    "mobile": ["HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER", "HAS_LENGTH:10"],
    "price": [],
    "paid_on": ["IS_DATETIME_PARSEABLE"],
}

### AVAILABLE RULES

- "HAS_ONLY_NUMERICAL_VALUES"
- "HAS_ONLY_POSITIVE_NUMERICAL_VALUES"
- "HAS_LENGTH:10"
- "HAS_MIN_LENGTH:7"
- "HAS_MAX_LENGTH:12"
- "HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER"
- "HAS_NO_EMPTY_STRINGS"
- "IS_DATETIME_PARSEABLE"
"#,
                );
                //continue;
                //Ok(())
                return Ok(());
            }

            if let Some(headers) = csv_builder.get_headers() {
                let mut json_array_str = "{\n".to_string();

                // Loop through headers and append them as keys in the JSON array string, excluding auto-computed columns
                for (i, header) in headers.iter().enumerate() {
                    if header != "id" && header != "c@" && header != "u@" {
                        json_array_str.push_str(&format!("    \"{}\": []", header));
                        if i < headers.len() - 1 {
                            json_array_str.push_str(",\n");
                        }
                    }
                }

                // Close the first JSON object and start the syntax explanation
                json_array_str.push_str("\n}");

                let syntax_explanation = r#"

SYNTAX
======

### Example

{
  "column1": ["HAS_ONLY_POSITIVE_NUMERICAL_VALUES", "HAS_NO_EMPTY_STRINGS"],
  "column2": [],
  "column3": ["HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER"],
  "column4": [],
  "column5": [],
  "column6": ["IS_DATETIME_PARSEABLE"],
  "column7": ["IS_DATETIME_PARSEABLE"]
}

### AVAILABLE RULES
- "HAS_ONLY_NUMERICAL_VALUES"
- "HAS_ONLY_POSITIVE_NUMERICAL_VALUES"
- "HAS_LENGTH:10"
- "HAS_MIN_LENGTH:7"
- "HAS_MAX_LENGTH:12"
- "HAS_VALID_TEN_DIGIT_INDIAN_MOBILE_NUMBER"
- "HAS_NO_EMPTY_STRINGS"
- "IS_DATETIME_PARSEABLE"

"#;

                let full_syntax = json_array_str + syntax_explanation;

                // Get user input
                let rows_json_str = get_edited_user_json_input(full_syntax);
                //dbg!(&rows_json_str);
                if handle_cancel_flag(&rows_json_str) {
                    //continue;
                    //Ok(())
                    return Ok(());
                }

                // Parse the user input
                let rows_json: Value = match serde_json::from_str(&rows_json_str) {
                    Ok(json) => json,
                    Err(e) => {
                        eprintln!("Error parsing JSON string: {}", e);
                        return Err("An error occurred".to_string().into());
                    }
                };

                // Collect rules from user input
                let mut rules = Vec::new();
                if let Some(obj) = rows_json.as_object() {
                    for (key, value) in obj {
                        if let Some(rules_array) = value.as_array() {
                            let mut column_rules = Vec::new();
                            for rule in rules_array {
                                if let Some(rule_str) = rule.as_str() {
                                    if !rule_str.is_empty() {
                                        column_rules.push(rule_str.to_string());
                                    }
                                }
                            }
                            if !column_rules.is_empty() {
                                rules.push((key.clone(), column_rules));
                            }
                        }
                    }
                }

                println!();
                // Invoke the cleanliness report function with the collected rules
                csv_builder
                    .print_cleanliness_report_by_column_parse(rules.clone())
                    .clean_by_column_parse(rules.clone());

                if csv_builder.has_data() {
                    csv_builder.print_table();
                    println!();
                }

                //    csv_builder.print_table();
                /*
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
        }

        _ => {
            println!("Invalid option. Please enter a number from 1 to 16.");
            //continue;
            //Ok(())
            return Ok(());
        } //        }

          //println!();
    }

    Ok(())
}
