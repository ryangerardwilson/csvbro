// csv_searcher.rs
use crate::user_experience::handle_cancel_flag;
use crate::user_interaction::{get_user_input_level_2, print_insight_level_2, print_list_level_2};
use rgwml::csv_utils::CsvBuilder;

pub async fn handle_search(
    mut csv_builder: CsvBuilder,
    _file_path_option: Option<&str>,
    action_type: &str,
    action_feature: &str,
    action_flag: &str,
    action_menu_options: Vec<&str>,
    _big_file_threshold: &str,
) -> Result<(CsvBuilder, bool), Box<dyn std::error::Error>> {
    match action_feature {
        "" => {
            let action_sub_menu_options = vec![
                "CONTAINS search",
                "CONTAINS (NOT) search",
                "STARTS WITH search",
                "STARTS WITH (NOT) search",
                "LEVENSHTEIN RAW search",
                "LEVENSHTEIN VECTORIZED search",
            ];

            print_list_level_2(&action_menu_options, &action_sub_menu_options, &action_type);

            return Ok((csv_builder, false));
        }

        "1" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to do a CONTAINS seach across all columns, or across specific columns.
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

  @LILbro: Enter search term: books
  @LILbro: Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): *

|id |item |value |type |date      |relates_to_travel |date_YEAR_MONTH |
-----------------------------------------------------------------------
|1  |books|1000  |OTHER|2024-01-21|0                 |Y2024-M01       |
|6  |books|1000  |OTHER|2024-03-21|0                 |Y2024-M03       |
Total rows: 2
"#,
                );
                return Ok((csv_builder, false));
            }

            let query = get_user_input_level_2("Enter search term: ");

            if handle_cancel_flag(&query) {
                return Ok((csv_builder, false));
            }

            // Step 2: Ask the user if they want to search all columns or specific ones
            let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): ");

            if handle_cancel_flag(&search_scope) {
                return Ok((csv_builder, false));
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
        }
        "2" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to do a 'CONTAINS NOT' seach across all columns, or across specific columns.
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

  @LILbro: Enter search term: books
  @LILbro: Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): item

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
Total rows: 8
"#,
                );
                return Ok((csv_builder, false));
            }

            let query = get_user_input_level_2("Enter search term: ");
            if handle_cancel_flag(&query) {
                //continue;
                return Ok((csv_builder, false));
            }

            // Prompt user for columns to search within or use all columns
            let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): ");
            // Check for @cancel to allow user to return to the main menu
            if handle_cancel_flag(&search_scope) {
                return Ok((csv_builder, false));
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
        }

        "3" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to do a 'STARTS WITH' seach across all columns, or across specific columns.
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

  @LILbro: Enter search term: boo
  @LILbro: Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): *

|id |item |value |type |date      |relates_to_travel |date_YEAR_MONTH |
-----------------------------------------------------------------------
|1  |books|1000  |OTHER|2024-01-21|0                 |Y2024-M01       |
|6  |books|1000  |OTHER|2024-03-21|0                 |Y2024-M03       |
Total rows: 2
"#,
                );
                return Ok((csv_builder, false));
            }

            let query = get_user_input_level_2("Enter search term: ");
            // Check for @cancel to allow user to return to the main menu
            if handle_cancel_flag(&query) {
                return Ok((csv_builder, false));
            }

            // Prompt user for columns to search within or use all columns
            let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): ");
            // Check for @cancel to allow user to return to the main menu
            if handle_cancel_flag(&search_scope) {
                return Ok((csv_builder, false));
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
        }
        "4" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to do a 'STARTS WITH NOT' seach across all columns, or across specific columns.
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

  @LILbro: Enter search term: boo
  @LILbro: Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): *

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
Total rows: 8
"#,
                );
                return Ok((csv_builder, false));
            }

            let query = get_user_input_level_2("Enter search term: ");
            // Check for @cancel to allow user to return to the main menu
            if handle_cancel_flag(&query) {
                return Ok((csv_builder, false));
            }

            // Prompt user for columns to search within or use all columns
            let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1, column2'): ");
            // Check for @cancel to allow user to return to the main menu
            if handle_cancel_flag(&search_scope) {
                return Ok((csv_builder, false));
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
        }
        "5" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to search through the data for rows that closely match a given search string based on the Levenshtein distance (a measure of how many single-character edits are required to change one word into another) and then sort and display these results along with statistics about the distances.
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

  @LILbro: Enter search query: boukz
  @LILbro: Enter Levenshtein distance (a non-negative integer): 7
  @LILbro: Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1,column2'): *

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|6  |books   |1000  |OTHER |2024-03-21|0                 |Y2024-M03       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
Total rows: 10

Distances (min to max): 2, 2, 4, 4, 5, 5, 5, 5, 5, 5

Mean: 4.20
Median: 5.00
Mode: 5
Frequencies:
  Distance 2: 2 occurrences
  Distance 4: 2 occurrences
  Distance 5: 6 occurrences
"#,
                );
                return Ok((csv_builder, false));
            }

            let query = get_user_input_level_2("Enter search query: ");
            if handle_cancel_flag(&query) {
                //continue;
                return Ok((csv_builder, false));
            }

            // Prompt user for the Levenshtein distance
            let lev_distance_input =
                get_user_input_level_2("Enter Levenshtein distance (a non-negative integer): ");
            // Check for @cancel to allow user to return to the main menu
            if handle_cancel_flag(&lev_distance_input) {
                //continue;
                return Ok((csv_builder, false));
            }

            let lev_distance = match lev_distance_input.trim().parse::<i32>() {
                Ok(distance) if distance >= 0 => distance as usize, // Ensure the distance is non-negative
                _ => {
                    println!("Error: The Levenshtein distance must be a non-negative integer.");
                    return Ok((csv_builder, false));
                }
            };

            // Prompt user for columns to search within or use all columns
            let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1,column2'): ");
            // Check for @cancel to allow user to return to the main menu
            if handle_cancel_flag(&search_scope) {
                return Ok((csv_builder, false));
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
        }

        "6" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Allows you to search through the data for rows that closely match a given search string based on the Levenshtein distance (a measure of how many single-character edits are required to change one word into another) and then sort and display these results along with statistics about the distances. However, unlike the 'LEVENSHTEIN RAW search' feature, this feature approaches the task differently in the following ways:
1. Multiple Search Strings: Unlike the 'LEVENSHTEIN RAW search' feature, which takes a single search string, this feature accepts a vector of search strings. It is designed to perform a search across multiple search strings simultaneously, finding the closest matches for any of the provided strings.
2. Handling Multiple Words: This feature splits each search string into words, counts them, and then searches for the closest match for the entire phrase (of that length of words) within the text of each cell. This involves a more complex comparison that can account for searches that are more phrase- or sentence-like, rather than single-word searches.
3. Windowed Search within Cells: For each cell in the dataset, this feature splits the cell's content into words and then performs a windowed search. This means it looks at all possible consecutive sequences of words within a cell that match the number of words in the search string, calculating the Levenshtein distance for each window of text. This approach allows for finding matches that are part of a larger text within a cell.
4. Finding the Minimum Distance Across Search Strings: This feature calculates the Levenshtein distance for each search string against each window of words in a cell and then takes the minimum distance found across all search strings for each row. This way, it identifies the closest match for any of the search strings provided.

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

  @LILbro: Enter queries separated by commas (e.g., 'needle1,needle2'): boukz, mouvez
  @LILbro: Enter Levenshtein distance (a non-negative integer): 7
  @LILbro: Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1,column2'): *

|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|6  |books   |1000  |OTHER |2024-03-21|0                 |Y2024-M03       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
Total rows: 10

Distances (min to max): 2, 2, 3, 3, 4, 4, 4, 4, 4, 4

Mean: 3.40
Median: 4.00
Mode: 4
Frequencies:
  Distance 2: 2 occurrences
  Distance 3: 2 occurrences
  Distance 4: 6 occurrences
"#,
                );
                return Ok((csv_builder, false));
            }

            // Prompt user for queries as a single string, expecting them to separate multiple queries with commas
            let queries_input = get_user_input_level_2(
                "Enter queries separated by commas (e.g., 'needle1, needle2'): ",
            );
            // Check for @cancel to allow user to return to the main menu
            if handle_cancel_flag(&queries_input) {
                return Ok((csv_builder, false));
            }

            let queries: Vec<&str> = queries_input
                .split(',')
                .map(|query| query.trim().trim_matches('"'))
                .collect();

            // Prompt user for the Levenshtein distance
            let lev_distance_input =
                get_user_input_level_2("Enter Levenshtein distance (a non-negative integer): ");
            // Check for @cancel to allow user to return to the main menu
            if handle_cancel_flag(&lev_distance_input) {
                return Ok((csv_builder, false));
            }

            let lev_distance = match lev_distance_input.trim().parse::<i32>() {
                Ok(distance) if distance >= 0 => distance as usize, // Ensure the distance is non-negative
                _ => {
                    println!("Error: The Levenshtein distance must be a non-negative integer.");
                    return Ok((csv_builder, false));
                }
            };

            // Prompt user for columns to search within or use all columns
            let search_scope = get_user_input_level_2("Type '*' to search all columns or list specific column names separated by commas (e.g., 'column1,column2'): ");
            // Check for @cancel to allow user to return to the main menu
            if handle_cancel_flag(&search_scope) {
                return Ok((csv_builder, false));
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
            csv_builder.print_vectorized_levenshtein_search_results(queries, lev_distance, columns);
            println!();
        }

        _ => {
            println!("Invalid option. Please enter a number from 1 to 6.");
            return Ok((csv_builder, false));
        }
    }

    println!();

    return Ok((csv_builder, true));
}
