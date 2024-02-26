// csv_searcher.rs
use crate::user_interaction::{
    determine_action_as_number, get_user_input_level_2, print_insight_level_2, print_list_level_2,
};
use rgwml::csv_utils::CsvBuilder;

pub fn handle_search(csv_builder: &mut CsvBuilder) -> Result<(), Box<dyn std::error::Error>> {
    let menu_options = vec![
        "CONTAINS search",
        "CONTAINS (NOT) search",
        "STARTS WITH search",
        "STARTS WITH (NOT) search",
        "LEVENSHTEIN RAW search",
        "LEVENSHTEIN VECTORIZED search",
        "Print all rows",
        "Go back",
    ];

    loop {
        print_insight_level_2("Select an option to search CSV data:");
        print_list_level_2(&menu_options);

        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        let selected_option = determine_action_as_number(&menu_options, &choice);

        match selected_option {
            Some(1) => {
                if csv_builder.has_data() {
                    let query = get_user_input_level_2("Enter search term: ");
                    csv_builder.print_contains_search_results(&query);
                    println!();
                }
            }
            Some(2) => {
                if csv_builder.has_data() {
                    let query = get_user_input_level_2("Enter search term: ");
                    csv_builder.print_not_contains_search_results(&query);
                    println!();
                }
            }
            Some(3) => {
                if csv_builder.has_data() {
                    let query = get_user_input_level_2("Enter search term: ");
                    csv_builder.print_starts_with_search_results(&query);
                    println!();
                }
            }
            Some(4) => {
                if csv_builder.has_data() {
                    let query = get_user_input_level_2("Enter search term: ");
                    csv_builder.print_not_starts_with_search_results(&query);
                    println!();
                }
            }
            Some(5) => {
                if csv_builder.has_data() {
                    let query_and_score = get_user_input_level_2(
                        r#"Enter comma separated values for query, lev_distance, search_cols. Syntax: "needle", 10, [column1, column2] // For specific cols, [*] for all cols: "#,
                    );

                    let parts: Vec<&str> = query_and_score.split(',').collect();

                    if parts.len() == 3 {
                        let query = parts[0].trim();
                        match parts[1].trim().parse::<i32>() {
                            Ok(score) if score >= 0 => {
                                let columns_part = parts[2].trim();
                                let columns = if columns_part == "*" {
                                    vec!["*"]
                                } else {
                                    columns_part
                                        .trim_matches(|c: char| c == '[' || c == ']')
                                        .split(',')
                                        .map(str::trim)
                                        .collect()
                                };

                                // Convert score to usize, ensuring it is non-negative
                                let max_lev_distance = score as usize;

                                // Call the function with the converted score
                                csv_builder.print_raw_levenshtein_search_results(
                                    query,
                                    max_lev_distance,
                                    columns,
                                );
                            }
                            Ok(_) => {
                                println!(
                                    "Error: The confidence score must be a non-negative integer."
                                );
                            }
                            Err(_) => {
                                println!("Error: The confidence score must be a valid integer.");
                            }
                        }
                    } else {
                        println!("Error: Please enter the search term, confidence score, and columns in the correct format. Ensure you are using commas to separate the values.");
                    }
                }
            }

            Some(6) => {
                if csv_builder.has_data() {
                    // This is a simplified approach without using regex for demonstration purposes.
                    let query_and_score = get_user_input_level_2(
                        r#"Enter comma separated values for queries, lev_distance, search_cols. Syntax: ["needle1", "needle2"], 10, [column1, column2] // For specific cols, [*] for all cols: "#,
                    );

                    // Manually extract the parts considering the structure of the input
                    if let Some(first_bracket_close) = query_and_score.find(']') {
                        let queries_part = &query_and_score[1..first_bracket_close]; // Extract between the first brackets
                        let remainder = &query_and_score[first_bracket_close + 2..]; // Skip over `], `
                        if let Some(last_comma) = remainder.rfind(',') {
                            let score_part = &remainder[..last_comma].trim();
                            let columns_part = &remainder[last_comma + 1..]
                                .trim()
                                .trim_matches(|c: char| c == '[' || c == ']');

                            let star = "*";

                            let queries: Vec<&str> = queries_part
                                .split("\", \"")
                                .map(|s| s.trim_matches('"'))
                                .collect();
                            let columns: Vec<&str> = if columns_part == &star {
                                vec!["*"]
                            } else {
                                columns_part
                                    .split(',')
                                    .map(|s| {
                                        s.trim().trim_matches(|c: char| {
                                            c == '"' || c == '[' || c == ']'
                                        })
                                    })
                                    .collect()
                            };

                            if let Ok(score) = score_part.parse::<i32>() {
                                if score >= 0 {
                                    // Convert score to usize, ensuring it is non-negative
                                    let max_lev_distance = score as usize;

                                    // Call the updated function with the vector of queries
                                    csv_builder.print_vectorized_levenshtein_search_results(
                                        queries,
                                        max_lev_distance,
                                        columns,
                                    );
                                } else {
                                    println!("Error: The Levenshtein distance must be a non-negative integer.");
                                }
                            } else {
                                println!(
                                    "Error: The Levenshtein distance must be a valid integer."
                                );
                            }
                        } else {
                            println!("Error: Incorrect format. Please check your input.");
                        }
                    } else {
                        println!("Error: Incorrect format. Please check your input.");
                    }
                }
            }

            Some(7) => {
                if csv_builder.has_data() {
                    csv_builder.print_table_all_rows();
                    println!();
                }
            }

            Some(8) => {
                break; // Exit the inspect handler
            }
            _ => {
                println!("Invalid option. Please enter a number from 1 to 8.");
                continue; // Ask for the choice again
            }
        }

        println!(); // Print a new line for better readability
    }

    Ok(())
}
