// csv_tinkerer.rs
use crate::user_interaction::{
    determine_action_as_number, get_edited_user_json_input, get_edited_user_sql_input,
    get_user_input_level_2, print_insight_level_2, print_list_level_2,
};
use rgwml::csv_utils::CsvBuilder;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::error::Error;

pub async fn handle_tinker(csv_builder: &mut CsvBuilder) -> Result<(), Box<dyn std::error::Error>> {
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

    let menu_options = vec![
        "SET HEADERS",
        "UPDATE HEADERS",
        "ADD ROWS",
        "UPDATE ROW",
        "EDIT TABLE (ASC)",
        "EDIT TABLE (DESC)",
        "DELETE ROWS",
        "ADD COLUMNS",
        "DROP COLUMNS",
        "RETAIN COLUMNS",
        "BACK",
    ];

    let original_csv_builder = CsvBuilder::from_copy(csv_builder);

    loop {
        print_insight_level_2("Select an option to search CSV data: ");
        print_list_level_2(&menu_options);

        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        let selected_option = determine_action_as_number(&menu_options, &choice);

        let prev_iteration_builder = CsvBuilder::from_copy(csv_builder);

        match selected_option {
            Some(1) => {
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
                    continue;
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

                let edited_json = get_edited_user_sql_input(headers_json_str);

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
                    continue;
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

                let edited_json = get_edited_user_sql_input(headers_json_str);

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

            Some(3) => {
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
                    continue;
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

                    // Parse the user input
                    let rows_json: Vec<serde_json::Value> =
                        match serde_json::from_str(&rows_json_str) {
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

                    csv_builder.add_rows(rows_as_str_slices);
                    csv_builder.print_table();
                    println!();
                    continue;
                } else {
                    print_insight_level_2("No headers set. Cannot add rows.");
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

            Some(4) => {
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
                    continue;
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

                if zero_based_index >= csv_builder.get_data().len() {
                    eprintln!("Row index out of range.");
                    //return;
                    return Err("An error occurred".to_string().into());
                }

                if let Some(existing_row) = csv_builder.get_data().get(zero_based_index) {
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

                        let edited_row: serde_json::Value = match serde_json::from_str(&edited_json)
                        {
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
                                        _ => edited_row[header]
                                            .as_str()
                                            .unwrap_or_default()
                                            .to_string(),
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
            Some(5) => {
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
                    continue;
                }

                let existing_data = csv_builder.get_data();

                let existing_headers: Vec<String> = csv_builder
                    .get_headers()
                    .unwrap_or(&[])
                    .iter()
                    .cloned() // Clone each String in the Vec
                    .collect();

                let mut json_array_str = "[".to_string();

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

            Some(6) => {
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
                    continue;
                }

                let existing_data = csv_builder.get_data();

                let existing_headers: Vec<String> = csv_builder
                    .get_headers()
                    .unwrap_or(&[])
                    .iter()
                    .cloned() // Clone each String in the Vec
                    .collect();
                let mut json_array_str = "[".to_string();

                for row in existing_data.iter().rev() {
                    // Iterate in reverse
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
                    json_array_str.pop(); // Remove trailing comma
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

            Some(7) => {
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
                    continue;
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

                let row_identifiers_str = get_user_input_level_2("Enter the identifiers (ID or indices) of the rows to delete (comma-separated), or type 'back' to return: ");
                println!();
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
            }

            Some(8) => {
                if choice.to_lowercase() == "8d" {
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
                    continue;
                }

                let new_columns_input = get_user_input_level_2("Enter new column names: ");
                println!();
                //let new_columns: Vec<&str> = new_columns_input.trim().split(',').collect();

                let new_columns: Vec<String> = new_columns_input
                    .trim() // Trim the whole input to remove leading/trailing whitespace around the input
                    .split(',') // Split the input into individual column names
                    .map(|name| name.trim().to_string()) // Trim each column name and convert to String
                    .collect();

                if !new_columns.is_empty() {
                    let existing_data = csv_builder.get_data();
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

                    let rows_json: Vec<serde_json::Value> =
                        match serde_json::from_str(&rows_json_str) {
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
                        if row_index < csv_builder.get_data().len() {
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
            }

            Some(9) => {
                if choice.to_lowercase() == "9d" {
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
                    continue;
                }

                let columns_input =
                    get_user_input_level_2("Please type a comma-separated list of columns: ");

                let columns: Vec<&str> =
                    columns_input.trim().split(',').map(|s| s.trim()).collect();

                csv_builder.drop_columns(columns).print_table();

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
            Some(10) => {
                if choice.to_lowercase() == "10d" {
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
                    continue;
                }

                let columns_input =
                    get_user_input_level_2("Please type a comma-separated list of columns: ");

                let columns: Vec<&str> =
                    columns_input.trim().split(',').map(|s| s.trim()).collect();

                csv_builder.retain_columns(columns).print_table();

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
            Some(11) => {
                csv_builder.print_table();

                break;
            }
            _ => {
                println!("Invalid option. Please enter a number from 1 to 11.");
                continue;
            }
        }

        //println!();
    }

    Ok(())
}
