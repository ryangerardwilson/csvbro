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
        println!();
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
                println!();
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
        "LEFT JOIN WITH",
        "RIGHT JOIN WITH",
        "SET INTERSECTION WITH",
        "SET DIFFERENCE WITH {1,2}",
        "SET SYMMETRIC DIFFERENCE WITH {1,2,4,5}",
        "BACK",
    ];

    let original_csv_builder = CsvBuilder::from_copy(csv_builder);

    loop {
        print_insight_level_2("Select an option to inspect CSV data:");
        print_list_level_2(&menu_options);
        let choice = get_user_input_level_2("Enter your choice: ").to_lowercase();
        let selected_option = determine_action_as_number(&menu_options, &choice);

        let csv_db_path = get_csv_db_path();
        let csv_db_path_buf = PathBuf::from(csv_db_path);

        let prev_iteration_builder = CsvBuilder::from_copy(csv_builder);

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
            }
            Some(3) => {
                if choice.to_lowercase() == "3d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

A LEFT JOIN on tables A and B includes every row from A along with any matching rows from B based on a join condition (i.e. a shared column). If there is no match in B for a row in A, the result still includes that row from A, with empty string values for the columns from B.


TABLE A
+++++++
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

TABLE B
+++++++
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A LEFT_JOIN B
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 26

|type  |implication    |
------------------------
|FOOD  |Daily Necessity|
|TRAVEL|Leisure        |
|OTHER |Misc.          |
Total rows: 3

  @LILbro: Enter column name from your above selected csv to LEFT JOIN at: type

|id |item    |value |type  |  <<+1 col>>   |relates_to_travel |date_YEAR_MONTH |implication    |
------------------------------------------------------------------------------------------------
|1  |books   |1000  |OTHER |...            |0                 |Y2024-M01       |Misc.          |
|2  |snacks  |200   |FOOD  |...            |0                 |Y2024-M02       |Daily Necessity|
|3  |cab fare|300   |TRAVEL|...            |1                 |Y2024-M03       |Leisure        |
|4  |rent    |20000 |OTHER |...            |0                 |Y2024-M01       |Misc.          |
|5  |movies  |1500  |OTHER |...            |0                 |Y2024-M02       |Misc.          |
|6  |books   |1000  |OTHER |...            |0                 |Y2024-M03       |Misc.          |
|7  |snacks  |200   |FOOD  |...            |0                 |Y2024-M01       |Daily Necessity|
|8  |cab fare|300   |TRAVEL|...            |1                 |Y2024-M02       |Leisure        |
|9  |rent    |20000 |OTHER |...            |0                 |Y2024-M03       |Misc.          |
|10 |movies  |1500  |OTHER |...            |0                 |Y2024-M01       |Misc.          |

Omitted columns: date
Total rows: 10
"#,
                    );
                    continue;
                }

                print_insight_level_2("Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A LEFT_JOIN B");
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                    let left_join_at_choice = get_user_input_level_2(
                        "Enter column name from your above selected csv to LEFT JOIN at: ",
                    )
                    .to_lowercase();

                    if left_join_at_choice.to_lowercase() == "@cancel" {
                        //return None;
                        return Ok(());
                    }

                    let union_type = format!("UNION_TYPE:LEFT_JOIN_AT_{}", left_join_at_choice);

                    //dbg!(&union_type);
                    csv_builder
                        .set_union_with(&chosen_file_path_for_join, &union_type)
                        .print_table();

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


            }
            Some(4) => {
                if choice.to_lowercase() == "4d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

A RIGHT JOIN between tables A and B is essentially the same as a LEFT JOIN between tables B and A, just approached from the opposite direction. Both join types aim to include all rows from one of the two tables being joined, regardless of whether there is a matching row in the other table. The difference lies in which table is guaranteed to have all its rows included:
- In a LEFT JOIN of A and B, every row from table A is included. If there's no matching row in B, the result will still include the row from A, with the columns from B filled with NULLs or placeholders.
- In a RIGHT JOIN of A and B, every row from table B is included. If there's no matching row in A, the result will still include the row from B, with the columns from A filled with NULLs or placeholders.

TABLE A
+++++++
|type  |implication    |
------------------------
|FOOD  |Daily Necessity|
|TRAVEL|Leisure        |
|OTHER |Misc.          |
Total rows: 3

TABLE B
+++++++
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A RIGHT_JOIN B
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 26

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

  @LILbro: Enter column name from your above selected csv to RIGHT JOIN at: type

|id |item    |value |type  |  <<+1 col>>   |relates_to_travel |date_YEAR_MONTH |implication    |
------------------------------------------------------------------------------------------------
|1  |books   |1000  |OTHER |...            |0                 |Y2024-M01       |Misc.          |
|2  |snacks  |200   |FOOD  |...            |0                 |Y2024-M02       |Daily Necessity|
|3  |cab fare|300   |TRAVEL|...            |1                 |Y2024-M03       |Leisure        |
|4  |rent    |20000 |OTHER |...            |0                 |Y2024-M01       |Misc.          |
|5  |movies  |1500  |OTHER |...            |0                 |Y2024-M02       |Misc.          |
|6  |books   |1000  |OTHER |...            |0                 |Y2024-M03       |Misc.          |
|7  |snacks  |200   |FOOD  |...            |0                 |Y2024-M01       |Daily Necessity|
|8  |cab fare|300   |TRAVEL|...            |1                 |Y2024-M02       |Leisure        |
|9  |rent    |20000 |OTHER |...            |0                 |Y2024-M03       |Misc.          |
|10 |movies  |1500  |OTHER |...            |0                 |Y2024-M01       |Misc.          |

Omitted columns: date
Total rows: 10
"#,
                    );
                    continue;
                }

                print_insight_level_2("Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A RIGHT_JOIN B");
                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                    let right_join_at_choice = get_user_input_level_2(
                        "Enter column name from your above selected csv to RIGHT JOIN at: ",
                    )
                    .to_lowercase();

                    if right_join_at_choice.to_lowercase() == "@cancel" {
                        //return None;
                        return Ok(());
                    }

                    let union_type = format!("UNION_TYPE:RIGHT_JOIN_AT_{}", right_join_at_choice);
                    csv_builder
                        .set_union_with(&chosen_file_path_for_join, &union_type)
                        .print_table();
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
            }
            Some(5) => {
                if choice.to_lowercase() == "5d" {
                    print_insight_level_2(
                        r#"DOCUMENTATION

A 'SET INTERSECTION WITH' analysis is useful to find common elements of data sets with similar column names but serving different purposes. For instance, if, instead of using a category column 'sales_type', a business decides to have two different csv files to record online_sales and instore_sales, a 'SET INTERSECTION WITH' analysis can help us find out which customers (identified uniquely in both files via an id column) shop online as well as at the store.


### Example 1

Background: A retail company operates both an online store and several physical locations. They have launched two separate marketing campaigns over the past month: one targeting online shoppers through digital ads (Campaign A) and another targeting in-store shoppers through traditional advertising methods (Campaign B). Each campaign aims to increase sales in its respective channel, but there is interest in understanding the overlap to refine future marketing strategies.

TABLE A
+++++++
@BIGBro: Opening z_online_sales.csv

|id |sales |date      |
-----------------------
|1  |120   |2024-03-01|
|2  |60    |2024-03-02|
|3  |200   |2024-03-03|
|4  |500   |2024-03-04|
|5  |300   |2024-03-05|
Total rows: 5

TABLE B
+++++++
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_INTERSECTION_WITH B
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 26
|id |sales |date      |
-----------------------
|6  |190   |2024-03-07|
|2  |40    |2024-03-08|
|3  |700   |2024-03-09|
|9  |100   |2024-03-10|
|5  |200   |2024-02-05|
Total rows: 5

  @LILbro: Enter column names (comma separated, if multiple) to SET_INTERSECTION_WITH at: id

|id |sales |date      |
-----------------------
|2  |40    |2024-03-08|
|3  |700   |2024-03-09|
|5  |200   |2024-02-05|
Total rows: 3

### Example 2

Background: Consider a scenario where a retail chain wants to perform a market basket analysis to understand shopping patterns across different store locations. The goal is to identify combinations of products that are frequently bought together by customers across multiple stores. In this case, there's no single customer_id or transaction_id that tracks purchases across stores, but a combination of category, item, and purchase_day can provide a unique enough signature to identify shopping patterns.

Background: The analysis aims to uncover products frequently bought together by customers across multiple stores of a retail chain. This insight is valuable for inventory management, marketing strategies, and enhancing customer satisfaction. TABLE A and TABLE B represent purchase records from two different stores. Each table lists products bought, categorized by `category` and `item`, along with the `purchase_day` of the week.

1. The operation SET_INTERSECTION_WITH at 'category, item, purchase_day': This command intersects TABLE A and TABLE B based on all three columns: `category`, `item`, and `purchase_day`. The intersection finds records where the exact combination of these three attributes matches across both tables, indicating the same item was purchased in the same category on the same day of the week in both stores.
2. The operation SET_INTERSECTION_WITH at 'category, item': This time, the intersection is performed on two columns: `category` and `item`, excluding `purchase_day`. This broader comparison reveals items that are commonly bought across stores regardless of the day they were purchased.
3. The operation SET_INTERSECTION_WITH at 'item, purchase_day': This command focuses on the intersection based on `item` and `purchase_day`, ignoring the `category`. This operation seeks to identify specific items bought on the same days across stores, potentially revealing day-specific purchasing trends for particular items.

TABLE A
+++++++
|category |item  |purchase_day |
--------------------------------
|Beverages|Tea   |Monday       |
|Bakery   |Bread |Tuesday      |
|Dairy    |Cheese|Wednesday    |
|Beverages|Coffee|Thursday     |
|Snacks   |Chips |Friday       |
|Beverages|Coffee|Monday       |
Total rows: 6

TABLE B
+++++++
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_INTERSECTION_WITH B
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 26
|category |item  |purchase_day |
--------------------------------
|Beverages|Tea   |Monday       |
|Bakery   |Bread |Tuesday      |
|Dairy    |Butter|Wednesday    |
|Beverages|Coffee|Thursday     |
|Snacks   |Nuts  |Friday       |
|Beverages|Tea   |Friday       |
Total rows: 6

  @LILbro: Enter column names (comma separated, if multiple) to SET_INTERSECTION_WITH at: category, item, pur
chase_day

|category |item  |purchase_day |
--------------------------------
|Beverages|Tea   |Monday       |
|Bakery   |Bread |Tuesday      |
|Beverages|Coffee|Thursday     |
Total rows: 3

  @LILbro: Enter column names (comma separated, if multiple) to SET_INTERSECTION_WITH at: category, item

|category |item  |purchase_day |
--------------------------------
|Beverages|Tea   |Monday       |
|Bakery   |Bread |Tuesday      |
|Beverages|Coffee|Thursday     |
|Beverages|Tea   |Friday       |
Total rows: 4

  @LILbro: Enter column names (comma separated, if multiple) to SET_INTERSECTION_WITH at: item, purchase_day

|category |item  |purchase_day |
--------------------------------
|Beverages|Tea   |Monday       |
|Bakery   |Bread |Tuesday      |
|Beverages|Coffee|Thursday     |
Total rows: 3
"#,
                    );
                    continue;
                }

                print_insight_level_2("Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_INTERSECTION_WITH B");

                let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);

                if let Some(ref chosen_file_path_for_join) = chosen_file_path_for_join {
                    let _ = CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                    println!();
                }

                if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                    // Capture user input for key columns
                    let set_intersection_at_choice = get_user_input_level_2(
        "Enter column names (comma separated, if multiple) to SET_INTERSECTION_WITH at: ",
    );

                    if set_intersection_at_choice.to_lowercase() == "@cancel" {
                        //return None;
                        return Ok(());
                    }

                    // Split the input string into a vector of &str, trimming whitespace and ignoring empty entries
                    let key_columns: Vec<&str> = set_intersection_at_choice
                        .split(',')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .collect();

                    // Ensure that there is at least one key column specified
                    if key_columns.is_empty() {
                        println!("Error: No key columns specified. Please specify at least one key column.");
                    } else {
                        // Perform set intersection with the specified key columns
                        csv_builder
                            .set_intersection_with(&chosen_file_path_for_join, key_columns)
                            .print_table();
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
