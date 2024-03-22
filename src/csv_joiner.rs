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

                let choice = get_user_input_level_2("Punch in the serial number or a slice of the file name to LOAD, or hit 'back' to bail.\nWhat's it gonna be?: \n\n(For the Universe U = {1,2,3,4,5,6,7}, A = {1,2,3} and B = {3,4,5})")
                .to_lowercase();

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

    /*
    let menu_options = vec![
        "Set union (all) with {1,2,3,3,4,5}",
        "Set union (all without duplicates) with {1,2,3,4,5}",
        "Set union (left join) with",
        "Set union (right join) with",
        "Set intersection with {3}",
        "Set difference with {1,2}",
        "Set symmetric difference with {1,2,4,5}",
        "Print all rows",
        "Go back",
    ];
    */
    let menu_options = vec![
        "SET UNION (ALL) WITH {1,2,3,3,4,5}",
        "SET UNION (ALL WITHOUT DUPLICATES) WITH {1,2,3,4,5}",
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
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    csv_builder
                        .set_union_with(&chosen_file_path_for_join, "UNION_TYPE:ALL")
                        .print_table();
                } else {
                    println!("No file was selected.");
                }
            }
            Some(2) => {
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    csv_builder
                        .set_union_with(
                            &chosen_file_path_for_join,
                            "UNION_TYPE:ALL_WITHOUT_DUPLICATES",
                        )
                        .print_table();
                } else {
                    println!("No file was selected.");
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
                } else {
                    println!("No file was selected.");
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
                } else {
                    println!("No file was selected.");
                }
            }
            Some(5) => {
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    csv_builder
                        .set_intersection_with(&chosen_file_path_for_join)
                        .print_table();
                } else {
                    println!("No file was selected.");
                }
            }
            Some(6) => {
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    csv_builder
                        .set_difference_with(&chosen_file_path_for_join)
                        .print_table();
                } else {
                    println!("No file was selected.");
                }
            }
            Some(7) => {
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    csv_builder
                        .set_symmetric_difference_with(&chosen_file_path_for_join)
                        .print_table();
                } else {
                    println!("No file was selected.");
                }
            }
            /*
                        Some(8) => {
                            if csv_builder.has_data() {
                                csv_builder.print_table_all_rows();
                                println!();
                            }
                        }
            */

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
