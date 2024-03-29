// csv_searcher.rs
use crate::user_interaction::{
    determine_action_as_number, get_user_input_level_2, print_insight_level_2, print_list_level_2,
};
use rgwml::csv_utils::CsvBuilder;

pub async fn handle_search(csv_builder: &mut CsvBuilder) -> Result<(), Box<dyn std::error::Error>> {
    fn apply_filter_changes_menu(
        csv_builder: &mut CsvBuilder,
        prev_iteration_builder: &CsvBuilder,
        original_csv_builder: &CsvBuilder,
    ) -> Result<(), String> {
        let menu_options = vec![
            "Continue with filtered data",
            "Discard this result, and load previous search result",
            "Load original, to search from scratch",
        ];
        print_insight_level_2("Apply changes?");
        print_list_level_2(&menu_options);

        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        let selected_option = determine_action_as_number(&menu_options, &choice);

        match selected_option {
            Some(1) => {
                print_insight_level_2("Continuing with filtered data");
                csv_builder.print_table();
                // Implement the logic for continuing with filtered data
                Ok(())
            }
            Some(2) => {
                print_insight_level_2("Discarding this result, and loading previous search result");
                csv_builder
                    .override_with(prev_iteration_builder)
                    .print_table();
                Ok(())
            }
            Some(3) => {
                print_insight_level_2("Loading original data, for you to search from scratch");
                csv_builder
                    .override_with(original_csv_builder)
                    .print_table();
                Ok(())
            }
            _ => Err("Invalid option. Please enter a number from 1 to 3.".to_string()),
        }
    }

    let menu_options = vec![
        "CONTAINS search",
        "CONTAINS (NOT) search",
        "STARTS WITH search",
        "STARTS WITH (NOT) search",
        "LEVENSHTEIN RAW search",
        "LEVENSHTEIN VECTORIZED search",
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
                let query = get_user_input_level_2("Enter search term: ");

                if query.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }

                // Step 2: Ask the user if they want to search all columns or specific ones
                let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): ");

                if search_scope.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }

                let columns: Vec<&str>;
                if search_scope.trim() == "*" {
                    // User wants to search all columns
                    columns = vec!["*"];
                } else {
                    // User provided specific column names
                    columns = search_scope.split(',').map(|s| s.trim()).collect();
                }

                // Step 3: Call the search method with the search term and columns
                csv_builder.print_contains_search_results(&query, columns);
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
            Some(2) => {
                let query = get_user_input_level_2("Enter search term: ");
                // Check for @cancel to allow user to return to the main menu
                if query.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }

                // Prompt user for columns to search within or use all columns
                let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): ");
                // Check for @cancel to allow user to return to the main menu
                if search_scope.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }

                let columns: Vec<&str>;
                if search_scope.trim() == "*" {
                    // User wants to search all columns
                    columns = vec!["*"];
                } else {
                    // User provided specific column names
                    columns = search_scope.split(',').map(|s| s.trim()).collect();
                }

                // Call the modified search method with the search term and specified columns
                csv_builder.print_not_contains_search_results(&query, columns);
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
            Some(3) => {
                let query = get_user_input_level_2("Enter search term: ");
                // Check for @cancel to allow user to return to the main menu
                if query.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }

                // Prompt user for columns to search within or use all columns
                let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): ");
                // Check for @cancel to allow user to return to the main menu
                if search_scope.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }

                let columns: Vec<&str>;
                if search_scope.trim() == "*" {
                    // User wants to search all columns
                    columns = vec!["*"];
                } else {
                    // User provided specific column names
                    columns = search_scope.split(',').map(|s| s.trim()).collect();
                }

                // Call the modified search method with the search term and specified columns
                csv_builder.print_starts_with_search_results(&query, columns);
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
            Some(4) => {
                let query = get_user_input_level_2("Enter search term: ");
                // Check for @cancel to allow user to return to the main menu
                if query.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }

                // Prompt user for columns to search within or use all columns
                let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): ");
                // Check for @cancel to allow user to return to the main menu
                if search_scope.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }

                let columns: Vec<&str>;
                if search_scope.trim() == "*" {
                    // User wants to search all columns
                    columns = vec!["*"];
                } else {
                    // User provided specific column names
                    columns = search_scope.split(',').map(|s| s.trim()).collect();
                }

                // Call the modified search method with the search term and specified columns
                csv_builder.print_not_starts_with_search_results(&query, columns);
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
                // Prompt user for the search query
                let query = get_user_input_level_2("Enter search query: ");
                // Check for @cancel to allow user to return to the main menu
                if query.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }

                // Prompt user for the Levenshtein distance
                let lev_distance_input =
                    get_user_input_level_2("Enter Levenshtein distance (a non-negative integer): ");
                // Check for @cancel to allow user to return to the main menu
                if lev_distance_input.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }
                let lev_distance = match lev_distance_input.trim().parse::<i32>() {
                    Ok(distance) if distance >= 0 => distance as usize, // Ensure the distance is non-negative
                    _ => {
                        println!("Error: The Levenshtein distance must be a non-negative integer.");
                        continue; // Return to the start of the loop for re-entry
                    }
                };

                // Prompt user for columns to search within or use all columns
                let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1,column2'): ");
                // Check for @cancel to allow user to return to the main menu
                if search_scope.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }

                let columns: Vec<&str>;
                if search_scope.trim() == "*" {
                    // User wants to search all columns
                    columns = vec!["*"];
                } else {
                    // User provided specific column names
                    columns = search_scope.split(',').map(|s| s.trim()).collect();
                }

                // Call the function with the user-provided values
                csv_builder.print_raw_levenshtein_search_results(&query, lev_distance, columns);
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

            Some(6) => {
                // Prompt user for queries as a single string, expecting them to separate multiple queries with commas
                let queries_input = get_user_input_level_2(
                    "Enter queries separated by commas (e.g., 'needle1,needle2'): ",
                );
                // Check for @cancel to allow user to return to the main menu
                if queries_input.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }
                let queries: Vec<&str> = queries_input
                    .split(',')
                    .map(|query| query.trim().trim_matches('"'))
                    .collect();

                // Prompt user for the Levenshtein distance
                let lev_distance_input =
                    get_user_input_level_2("Enter Levenshtein distance (a non-negative integer): ");
                // Check for @cancel to allow user to return to the main menu
                if lev_distance_input.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }
                let lev_distance = match lev_distance_input.trim().parse::<i32>() {
                    Ok(distance) if distance >= 0 => distance as usize, // Ensure the distance is non-negative
                    _ => {
                        println!("Error: The Levenshtein distance must be a non-negative integer.");
                        continue; // Return to the start of the loop for re-entry
                    }
                };

                // Prompt user for columns to search within or use all columns
                let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1,column2'): ");
                // Check for @cancel to allow user to return to the main menu
                if search_scope.to_lowercase() == "@cancel" {
                    continue; // Skip the current iteration and return to the main menu
                }

                let columns: Vec<&str> = if search_scope.trim() == "*" {
                    // User wants to search all columns
                    vec!["*"]
                } else {
                    // User provided specific column names
                    search_scope
                        .split(',')
                        .map(|s| s.trim().trim_matches('"'))
                        .collect()
                };

                // Call the function with the user-provided values
                csv_builder.print_vectorized_levenshtein_search_results(
                    queries,
                    lev_distance,
                    columns,
                );
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

            Some(7) => {
                csv_builder.print_table();

                break;
            }
            _ => {
                println!("Invalid option. Please enter a number from 1 to 7.");
                continue;
            }
        }

        println!();
    }

    Ok(())
}
