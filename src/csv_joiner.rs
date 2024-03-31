// csv_joiner.rs
use crate::user_interaction::{
    determine_action_as_number, get_user_input_level_2, print_insight_level_2, print_list_level_2,
};
use fuzzywuzzy::fuzz;
use rgwml::csv_utils::CsvBuilder;
use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

pub fn handle_join(csv_builder: &mut CsvBuilder) -> Result<(), Box<dyn std::error::Error>> {
    fn get_csv_db_path() -> String {
        let home_dir = env::var("HOME").expect("Unable to determine user home directory");
        let desktop_path = Path::new(&home_dir).join("Desktop");
        let csv_db_path = desktop_path.join("csv_db");

        return csv_db_path.to_string_lossy().into_owned();
    }

    pub fn select_csv_file_path(csv_db_path: &PathBuf) -> Option<String> {
        fn list_csv_files(path: &Path) -> io::Result<Vec<PathBuf>> {
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
                    println!("No files in sight, bro.");
                    return None;
                }

                files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

                for (index, file) in files.iter().enumerate() {
                    if let Some(file_name) = file.file_name().and_then(|n| n.to_str()) {
                        println!("{}: {}", index + 1, file_name);
                    }
                }

                let choice = get_user_input_level_2(
                    "Punch in the serial number or a slice of the file name to LOAD: ",
                )
                .to_lowercase();
                if choice.to_lowercase() == "@cancel" {
                    return None;
                }

                // First, try to parse the choice as a number to select by index
                if let Ok(index) = choice.parse::<usize>() {
                    if index > 0 && index <= files.len() {
                        // Adjust for 0-based indexing
                        return files
                            .get(index - 1)
                            .and_then(|path| path.to_str().map(String::from));
                    }
                }

                // If not a number or out of bounds, proceed with fuzzy matching
                let best_match_result = files
                    .iter()
                    .filter_map(|path| {
                        path.file_name()
                            .and_then(|n| n.to_str())
                            .map(|name| (path, fuzz::ratio(&choice, name)))
                    })
                    .max_by_key(|&(_, score)| score);

                if let Some((best_match, score)) = best_match_result {
                    if score > 60 {
                        // Assuming a threshold of 60 for a good match
                        return best_match.to_str().map(String::from);
                    }
                }

                println!("No matching file found.");
            }
            Err(_) => println!("Failed to read the directory."),
        }
        None
    }
    fn sort_csv_by_id_if_needed(csv_builder: &mut CsvBuilder) {
        let mut perform_sort = false;

        if let Some(headers) = csv_builder.get_headers() {
            for header in headers.iter() {
                if header == "id" {
                    perform_sort = true;
                    break; // No need to continue once we've found an "id" header
                }
            }
        }

        if perform_sort {
            let _ = csv_builder.cascade_sort(vec![("id".to_string(), "ASC".to_string())]);
        }
    }

    let menu_options = vec![
        "SET BAG UNION WITH",
        "SET UNION WITH",
        "SET UNION (LEFT JOIN) WITH",
        "SET UNION (RIGHT JOIN) WITH",
        "SET INTERSECTION WITH {3}",
        "SET DIFFERENCE WITH {1,2}",
        "SET SYMMETRIC DIFFERENCE WITH {1,2,4,5}",
        "BACK",
    ];

    loop {
        print_insight_level_2("Select an option to inspect CSV data:");
        print_list_level_2(&menu_options);
        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        let selected_option = determine_action_as_number(&menu_options, &choice);

        let csv_db_path = get_csv_db_path();
        let csv_db_path_buf = PathBuf::from(csv_db_path);

        match selected_option {
            Some(1) => {
                if choice.to_lowercase() == "1d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Computes A U B, whilst retaining duplicates. This operation is known as the "multiset union" or "bag union" in mathematics and computer science. Unlike the traditional set union, which produces a set that contains all of the elements from both sets without duplicates, a multiset union retains duplicates, reflecting the combined multiplicity of each element from both multisets. For A = {1,2,3} and B = {3,4,5}, it returns {1,2,3,3,4,5}

NOTE: This method will automatically sort the end result in ascending order of the id column.

TABLE A
+++++++
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
<<+2 rows>>
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|11 |concert |2000  |OTHER |2024-03-27|0                 |Y2024-M03       |
|12 |alcohol |1100  |OTHER |2024-03-28|0                 |Y2024-M03       |
Total rows: 12

TABLE B
+++++++
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
<<+2 rows>>
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|11 |concert |2000  |OTHER |2024-03-27|0                 |Y2024-M03       |
|12 |alcohol |1100  |OTHER |2024-03-28|0                 |Y2024-M03       |
Total rows: 12

  @LILbro: Punch in the serial number or a slice of the file name to LOAD, or hit 'back' to bail.
What's it gonna be?: test2
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
"#,
                    );
                    continue;
                }

                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    let _ =
                        csv_builder.set_union_with(&chosen_file_path_for_join, "UNION_TYPE:ALL");

                    sort_csv_by_id_if_needed(csv_builder);

                    csv_builder.print_table();
                }
            }
            Some(2) => {
                if choice.to_lowercase() == "2d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

Computes A U B, under traditional set theory. For A = {1,2,3} and B = {3,4,5}, it returns {1,2,3,4,5}

NOTE: This method will automatically sort the end result in ascending order of the id column.

TABLE A
+++++++
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|5  |movies  |1500  |OTHER |2024-02-25|0                 |Y2024-M02       |
<<+2 rows>>
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
|11 |concert |2000  |OTHER |2024-03-27|0                 |Y2024-M03       |
|12 |alcohol |1100  |OTHER |2024-03-28|0                 |Y2024-M03       |
Total rows: 12

TABLE B
+++++++
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |bags    |500   |TRAVEL|2024-03-28|1                 |Y2024-M03       |
Total rows: 4

  @LILbro: Punch in the serial number or a slice of the file name to LOAD: test
|id |item    |value |type  |date      |relates_to_travel |date_YEAR_MONTH |
---------------------------------------------------------------------------
|1  |books   |1000  |OTHER |2024-01-21|0                 |Y2024-M01       |
|2  |snacks  |200   |FOOD  |2024-02-22|0                 |Y2024-M02       |
|3  |cab fare|300   |TRAVEL|2024-03-23|1                 |Y2024-M03       |
|4  |rent    |20000 |OTHER |2024-01-24|0                 |Y2024-M01       |
|4  |bags    |500   |TRAVEL|2024-03-28|1                 |Y2024-M03       |
<<+1 row>>
|6  |books   |1000  |OTHER |2024-03-21|0                 |Y2024-M03       |
|7  |snacks  |200   |FOOD  |2024-01-22|0                 |Y2024-M01       |
|8  |cab fare|300   |TRAVEL|2024-02-23|1                 |Y2024-M02       |
|9  |rent    |20000 |OTHER |2024-03-24|0                 |Y2024-M03       |
|10 |movies  |1500  |OTHER |2024-01-25|0                 |Y2024-M01       |
Total rows: 11
"#,
                    );
                    continue;
                }

                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    let _ = csv_builder.set_union_with(
                        &chosen_file_path_for_join,
                        "UNION_TYPE:ALL_WITHOUT_DUPLICATES",
                    );

                    sort_csv_by_id_if_needed(csv_builder);

                    csv_builder.print_table();
                }
            }
            Some(3) => {
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                    let left_join_at_choice = get_user_input_level_2(
                        "Enter column name from your above selected csv to LEFT JOIN at: ",
                    )
                    .to_lowercase();
                    let union_type = format!("UNION_TYPE:LEFT_JOIN_AT{{{}}}", left_join_at_choice);
                    csv_builder
                        .set_union_with(&chosen_file_path_for_join, &union_type)
                        .print_table();
                }
            }
            Some(4) => {
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                    let left_join_at_choice = get_user_input_level_2(
                        "Enter column name from your above selected csv to RIGHT JOIN at: ",
                    )
                    .to_lowercase();
                    let union_type = format!("UNION_TYPE:RIGHT_JOIN_AT{{{}}}", left_join_at_choice);
                    csv_builder
                        .set_union_with(&chosen_file_path_for_join, &union_type)
                        .print_table();
                }
            }
            Some(5) => {
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    csv_builder
                        .set_intersection_with(&chosen_file_path_for_join)
                        .print_table();
                }
            }
            Some(6) => {
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    csv_builder
                        .set_difference_with(&chosen_file_path_for_join)
                        .print_table();
                }
            }
            Some(7) => {
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    csv_builder
                        .set_symmetric_difference_with(&chosen_file_path_for_join)
                        .print_table();
                }
            }
            Some(8) => {
                csv_builder.print_table();
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
