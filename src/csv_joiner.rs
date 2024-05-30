// csv_joiner.rs
use crate::user_experience::handle_cancel_flag;
use crate::user_interaction::{get_user_input_level_2, print_insight_level_2, print_list_level_2};
use fuzzywuzzy::fuzz;
use rgwml::csv_utils::CsvBuilder;
use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

pub async fn handle_join(
    mut csv_builder: CsvBuilder,
    _file_path_option: Option<&str>,
    action_feature: &str,
    action_flag: &str,
) -> Result<(CsvBuilder, bool), Box<dyn std::error::Error>> {
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
                if handle_cancel_flag(&choice) {
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

    let csv_db_path = get_csv_db_path();
    let csv_db_path_buf = PathBuf::from(csv_db_path);

    match action_feature {
        "" => {
            print_insight_level_2("Here's the JOIN feature menu ... ");
            let menu_options = vec![
                "UNION",
                "UNION (BAG)",
                "UNION (LEFT JOIN/ OUTER LEFT JOIN)",
                "UNION (RIGHT JOIN/ OUTER RIGHT JOIN)",
                "UNION (OUTER FULL JOIN)",
                "INTERSECTION",
                "INTERSECTION (INNER JOIN)",
                "DIFFERENCE",
                "DIFFERENCE (SYMMETRIC)",
            ];

            print_list_level_2(&menu_options);

            return Ok((csv_builder, false));
        }

        "1" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Computes A U B, under traditional set theory. For A = {1,2,3} and B = {3,4,5}, it returns {1,2,3,4,5}

NOTE: 
1. This method will automatically sort the end result in ascending order of the id column.
2. While eliminating duplicates - the preference for inclusion is based first on the dataset (with the earlier dataset being preferred) and then on the row order within that dataset.

### Example 1

TABLE A
+++++++
@BIGBro: Opening z_instore_sales.csv

|id |sales |date      |
-----------------------
|6  |190   |2024-03-07|
|2  |40    |2024-03-08|
|3  |700   |2024-03-09|
|9  |100   |2024-03-10|
|5  |200   |2024-02-05|
Total rows: 5

TABLE B
+++++++
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 52

|id |sales |date      |
-----------------------
|1  |120   |2024-03-01|
|2  |60    |2024-03-02|
|3  |200   |2024-03-03|
|4  |500   |2024-03-04|
|5  |300   |2024-03-05|
Total rows: 5

  @LILbro: Enter comma separated column name/names from your above selected csvs to determine uniqueness by (use * for a traditional union that determines uniqueness based on all columns)): *

|id |sales |date      |
-----------------------
|1  |120   |2024-03-01|
|2  |40    |2024-03-08|
|2  |60    |2024-03-02|
|3  |700   |2024-03-09|
|3  |200   |2024-03-03|
|4  |500   |2024-03-04|
|5  |200   |2024-02-05|
|5  |300   |2024-03-05|
|6  |190   |2024-03-07|
|9  |100   |2024-03-10|
Total rows: 10

### Example 2

Should you decide to determine uniqueness by a specific column, or a specific combination of columns - the preference for inclusion is based first on the dataset (with the earlier dataset being preferred) and then on the row order within that dataset. In the below example, for id 5, the row with id 5 in the first data set is retained.

TABLE A
+++++++
@BIGBro: Opening z_instore_sales.csv

|id |sales |date      |
-----------------------
|6  |190   |2024-03-07|
|2  |40    |2024-03-08|
|3  |700   |2024-03-09|
|9  |100   |2024-03-10|
|5  |200   |2024-02-05|
Total rows: 5

TABLE B
+++++++
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 52

|id |sales |date      |
-----------------------
|1  |120   |2024-03-01|
|2  |60    |2024-03-02|
|3  |200   |2024-03-03|
|4  |500   |2024-03-04|
|5  |300   |2024-03-05|
Total rows: 5

  @LILbro: Enter comma separated column name/names from your above selected csvs to determine uniqueness by (use * for a traditional union that determines uniqueness based on all columns)): id

|id |sales |date      |
-----------------------
|1  |120   |2024-03-01|
|2  |40    |2024-03-08|
|3  |700   |2024-03-09|
|4  |500   |2024-03-04|
|5  |200   |2024-02-05|
|6  |190   |2024-03-07|
|9  |100   |2024-03-10|
Total rows: 7
"#,
                );
                return Ok((csv_builder, false));
            }

            let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
            if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();

                let join_at_choice = get_user_input_level_2(
                        "Enter comma separated column name/names from your above selected csvs to determine uniqueness by (use * for a traditional union that determines uniqueness based on all columns)): ",
                    )
                    .to_lowercase();

                if handle_cancel_flag(&join_at_choice) {
                    return Ok((csv_builder, false));
                }

                let column_names: Vec<&str> = join_at_choice.split(',').map(|s| s.trim()).collect();

                let _ = csv_builder.set_union_with_csv_file(
                    &chosen_file_path_for_join,
                    "UNION_TYPE:NORMAL",
                    column_names,
                );

                sort_csv_by_id_if_needed(&mut csv_builder);

                csv_builder.print_table();
            }
        }

        "2" => {
            if action_flag == "d" {
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
                return Ok((csv_builder, false));
            }

            let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);

            if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                println!();
                print_insight_level_2("Now, computing the bag union with the above ...");
                let _ = csv_builder.set_bag_union_with_csv_file(&chosen_file_path_for_join);

                sort_csv_by_id_if_needed(&mut csv_builder);

                csv_builder.print_table();
            }
        }

        "3" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

A LEFT JOIN on tables A and B includes every row from A along with any matching rows from B based on a join condition (i.e. a shared column). If there is no match in B for a row in A, the result still includes that row from A, with empty string values for the columns from B.


TABLE A
+++++++
|id |value |date      |interest |type  |
----------------------------------------
|1  |500   |2024-04-08|7        |FOOD  |
|2  |450   |2024-04-07|8        |FOOD  |
|3  |420   |2024-04-06|9        |TRAVEL|
|4  |400   |2024-04-05|7        |OTHER |
|5  |380   |2024-04-05|7.2      |TRAVEL|
|6  |360   |2024-04-03|8.2      |OTHER |
|7  |340   |2024-04-02|9.2      |FOOD  |
|8  |320   |2024-04-01|7.4      |TRAVEL|
|9  |300   |2024-04-08|8.4      |FOOD  |
|10 |280   |2024-04-08|9.4      |FOOD  |
Total rows: 10

TABLE B
+++++++
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A LEFT_JOIN B
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 26

|id |type  |implication    |
----------------------------
|1  |FOOD  |Daily Necessity|
|2  |TRAVEL|Leisure        |
|3  |OTHER |Misc.          |
Total rows: 3

  @LILbro: Enter column name from your above selected csv to LEFT JOIN at: type

|id |value |date      |interest |type  |joined_id |joined_implication |
-----------------------------------------------------------------------
|1  |500   |2024-04-08|7        |FOOD  |1         |Daily Necessity    |
|2  |450   |2024-04-07|8        |FOOD  |1         |Daily Necessity    |
|3  |420   |2024-04-06|9        |TRAVEL|2         |Leisure            |
|4  |400   |2024-04-05|7        |OTHER |3         |Misc.              |
|5  |380   |2024-04-05|7.2      |TRAVEL|2         |Leisure            |
|6  |360   |2024-04-03|8.2      |OTHER |3         |Misc.              |
|7  |340   |2024-04-02|9.2      |FOOD  |1         |Daily Necessity    |
|8  |320   |2024-04-01|7.4      |TRAVEL|2         |Leisure            |
|9  |300   |2024-04-08|8.4      |FOOD  |1         |Daily Necessity    |
|10 |280   |2024-04-08|9.4      |FOOD  |1         |Daily Necessity    |
Total rows: 10
"#,
                );
                return Ok((csv_builder, false));
            }

            print_insight_level_2("Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A LEFT_JOIN B");
            let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
            if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                let left_join_at_choice = get_user_input_level_2(
                        "Enter comma-separated column name/names from your above selected csv to LEFT JOIN at: ",
                    )
                    .to_lowercase();

                if handle_cancel_flag(&left_join_at_choice) {
                    return Ok((csv_builder, false));
                }

                let column_names: Vec<&str> =
                    left_join_at_choice.split(',').map(|s| s.trim()).collect();

                //dbg!(&union_type);
                csv_builder
                    .set_union_with_csv_file(
                        &chosen_file_path_for_join,
                        "UNION_TYPE:LEFT_JOIN",
                        column_names,
                    )
                    .print_table();
            }
        }

        "4" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

A RIGHT JOIN between tables A and B is essentially the same as a LEFT JOIN between tables B and A, just approached from the opposite direction. Both join types aim to include all rows from one of the two tables being joined, regardless of whether there is a matching row in the other table. The difference lies in which table is guaranteed to have all its rows included:
- In a LEFT JOIN of A and B, every row from table A is included. If there's no matching row in B, the result will still include the row from A, with the columns from B filled with NULLs or placeholders.
- In a RIGHT JOIN of A and B, every row from table B is included. If there's no matching row in A, the result will still include the row from B, with the columns from A filled with NULLs or placeholders.

TABLE A
+++++++
|id |type  |implication    |
----------------------------
|1  |FOOD  |Daily Necessity|
|2  |TRAVEL|Leisure        |
|3  |OTHER |Misc.          |
Total rows: 3

TABLE B
+++++++
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A RIGHT_JOIN B
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 26

|id |value |date      |interest |type  |
----------------------------------------
|1  |500   |2024-04-08|7        |FOOD  |
|2  |450   |2024-04-07|8        |FOOD  |
|3  |420   |2024-04-06|9        |TRAVEL|
|4  |400   |2024-04-05|7        |OTHER |
|5  |380   |2024-04-05|7.2      |TRAVEL|
|6  |360   |2024-04-03|8.2      |OTHER |
|7  |340   |2024-04-02|9.2      |FOOD  |
|8  |320   |2024-04-01|7.4      |TRAVEL|
|9  |300   |2024-04-08|8.4      |FOOD  |
|10 |280   |2024-04-08|9.4      |FOOD  |
Total rows: 10

  @LILbro: Enter column name from your above selected csv to RIGHT JOIN at: type

|joined_id |joined_value |joined_date |joined_interest |type  |id |implication    |
-----------------------------------------------------------------------------------
|1         |500          |2024-04-08  |7               |FOOD  |1  |Daily Necessity|
|2         |450          |2024-04-07  |8               |FOOD  |1  |Daily Necessity|
|3         |420          |2024-04-06  |9               |TRAVEL|2  |Leisure        |
|4         |400          |2024-04-05  |7               |OTHER |3  |Misc.          |
|5         |380          |2024-04-05  |7.2             |TRAVEL|2  |Leisure        |
|6         |360          |2024-04-03  |8.2             |OTHER |3  |Misc.          |
|7         |340          |2024-04-02  |9.2             |FOOD  |1  |Daily Necessity|
|8         |320          |2024-04-01  |7.4             |TRAVEL|2  |Leisure        |
|9         |300          |2024-04-08  |8.4             |FOOD  |1  |Daily Necessity|
|10        |280          |2024-04-08  |9.4             |FOOD  |1  |Daily Necessity|
Total rows: 10
"#,
                );
                return Ok((csv_builder, false));
            }

            print_insight_level_2("Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A RIGHT_JOIN B");
            let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);
            if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                let right_join_at_choice = get_user_input_level_2(
                        "Enter comma separated column name/ names from your above selected csv to RIGHT JOIN at: ",
                    )
                    .to_lowercase();

                if handle_cancel_flag(&right_join_at_choice) {
                    return Ok((csv_builder, false));
                }

                let column_names: Vec<&str> =
                    right_join_at_choice.split(',').map(|s| s.trim()).collect();

                csv_builder
                    .set_union_with_csv_file(
                        &chosen_file_path_for_join,
                        "UNION_TYPE:RIGHT_JOIN",
                        column_names,
                    )
                    .print_table();
            }
        }

        "5" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION
An OUTER FULL JOIN between tables A and B combines the results of both a left join and a right join. This join type includes every row from both tables A and B. If a row from table A matches one from table B based on a join condition (typically a shared column), the joined table will include columns from both rows. If there is no match:
  - For a row in table A, the row will include this data with null (or similar placeholder) values for the columns from table B.
  - Conversely, if there is a row in table B that does not match any in table A, the row will include this data with null values for the columns from table A.

TABLE A
+++++++
|id |value |date      |interest |type  |
----------------------------------------
|1  |500   |2024-04-08|7        |FOOD  |
|2  |450   |2024-04-07|8        |FOOD  |
|3  |420   |2024-04-06|9        |TRAVEL|
|4  |400   |2024-04-05|7        |OTHER |
|5  |380   |2024-04-05|7.2      |TRAVEL|
|6  |360   |2024-04-03|8.2      |OTHER |
|7  |340   |2024-04-02|9.2      |FOOD  |
|8  |320   |2024-04-01|7.4      |TRAVEL|
|9  |300   |2024-04-08|8.4      |FOOD  |
|10 |280   |2024-04-08|9.4      |FOOD  |
Total rows: 10

TABLE B
+++++++
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A LEFT_JOIN B
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 26

|id |type    |implication    |
------------------------------
|1  |FOOD    |Daily Necessity|
|2  |TRAVEL  |Leisure        |
|3  |OTHER   |Misc.          |
|4  |BUSINESS|Refund         |
Total rows: 4

  @LILbro: Enter column name from your above selected csv to LEFT JOIN at: type

  |id |value |date      |interest |type    |joined_id |joined_implication |
-------------------------------------------------------------------------
|1  |500   |2024-04-08|7        |FOOD    |1         |Daily Necessity    |
|2  |450   |2024-04-07|8        |FOOD    |1         |Daily Necessity    |
|3  |420   |2024-04-06|9        |TRAVEL  |2         |Leisure            |
|4  |400   |2024-04-05|7        |OTHER   |3         |Misc.              |
|5  |380   |2024-04-05|7.2      |TRAVEL  |2         |Leisure            |
<<+1 row>>
|7  |340   |2024-04-02|9.2      |FOOD    |1         |Daily Necessity    |
|8  |320   |2024-04-01|7.4      |TRAVEL  |2         |Leisure            |
|9  |300   |2024-04-08|8.4      |FOOD    |1         |Daily Necessity    |
|10 |280   |2024-04-08|9.4      |FOOD    |1         |Daily Necessity    |
|   |      |          |         |BUSINESS|4         |Refund             |
Total rows: 11
"#,
                );
                return Ok((csv_builder, false));
            }

            print_insight_level_2("Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_OUTER_FULL_JOIN_UNION_WITH B");

            let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);

            if let Some(ref chosen_file_path_for_join) = chosen_file_path_for_join {
                let _ = CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                println!();
            }

            if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                // Capture user input for key columns
                let set_intersection_at_choice = get_user_input_level_2(
        "Enter column names (comma separated, if multiple) to SET_OUTER_FULL_JOIN_UNION_WITH at: ",
    );

                if handle_cancel_flag(&set_intersection_at_choice) {
                    return Ok((csv_builder, false));
                }

                // Split the input string into a vector of &str, trimming whitespace and ignoring empty entries
                let key_columns: Vec<&str> = set_intersection_at_choice
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect();

                // Ensure that there is at least one key column specified
                if key_columns.is_empty() {
                    println!(
                        "Error: No key columns specified. Please specify at least one key column."
                    );
                } else {
                    // Perform set intersection with the specified key columns
                    csv_builder
                        .set_union_with_csv_file(
                            &chosen_file_path_for_join,
                            "UNION_TYPE:OUTER_FULL_JOIN",
                            key_columns,
                        )
                        .print_table();
                }
            }
        }

        "6" => {
            if action_flag == "d" {
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

The analysis aims to uncover products frequently bought together by customers across multiple stores of a retail chain. This insight is valuable for inventory management, marketing strategies, and enhancing customer satisfaction. TABLE A and TABLE B represent purchase records from two different stores. Each table lists products bought, categorized by `category` and `item`, along with the `purchase_day` of the week:
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
                return Ok((csv_builder, false));
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

                if handle_cancel_flag(&set_intersection_at_choice) {
                    return Ok((csv_builder, false));
                }

                // Split the input string into a vector of &str, trimming whitespace and ignoring empty entries
                let key_columns: Vec<&str> = set_intersection_at_choice
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect();

                // Ensure that there is at least one key column specified
                if key_columns.is_empty() {
                    println!(
                        "Error: No key columns specified. Please specify at least one key column."
                    );
                } else {
                    // Perform set intersection with the specified key columns
                    csv_builder
                        .set_intersection_with_csv_file(
                            &chosen_file_path_for_join,
                            key_columns,
                            "INTERSECTION_TYPE:NORMAL",
                        )
                        .print_table();
                }
            }
        }

        "7" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

A 'INTERSECTION (INNER JOIN)' is used to combine data from two distinct sets based on a common attribute or condition, effectively finding the intersection between these sets. This method is particularly useful when you're dealing with data that is related but stored in separate sources. For example, if a business maintains separate datasets for online sales and in-store sales, each identified by a unique customer ID, an INNER JOIN can merge these datasets to focus exclusively on customers who appear in both. This enables a comprehensive analysis of cross-channel shopping behaviors by filtering out customers who have only shopped through one channel. The result of an INNER JOIN provides a concentrated view of shared data points, making it ideal for identifying patterns or relationships that only exist across intersecting subsets of data.

### Example 1

Background: A retail company wants to analyze customer shopping behavior across different sales channels: online and in-store. They have separate datasets for transactions made online and those made at physical store locations. The goal is to create a 'profile' table for customers who shop both online and in-store to target them with integrated marketing campaigns.

TABLE A
+++++++
@BIGBro: Opening zzzz_online_sales.csv

|customer_id |online_sales |online_date |
-----------------------------------------
|1           |120          |2024-03-01  |
|2           |60           |2024-03-02  |
|3           |200          |2024-03-03  |
|4           |500          |2024-03-04  |
|5           |300          |2024-03-05  |
Total rows: 5

TABLE B
+++++++
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_INNER_JOIN_INTERSECTION_WITH B
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 26

|customer_id |instore_sales |instore_date |
-------------------------------------------
|6           |190           |2024-03-07   |
|2           |40            |2024-03-08   |
|3           |700           |2024-03-09   |
|9           |100           |2024-03-10   |
|5           |200           |2024-03-05   |
Total rows: 5

  @LILbro: Enter column names (comma separated, if multiple) to SET_INTERSECTION_WITH at: customer_id

|customer_id |online_sales |online_date |instore_sales |instore_date |
----------------------------------------------------------------------
|2           |60           |2024-03-02  |40            |2024-03-08   |
|3           |200          |2024-03-03  |700           |2024-03-09   |
|5           |300          |2024-03-05  |200           |2024-03-05   |
Total rows: 3

### Example 2

Background: A publishing company is looking to understand the overlap between its digital and print subscription bases. It maintains separate records for customers' digital and print subscriptions, including start dates, to assess cross-media preferences. The aim is to integrate these datasets to identify customers with both digital and print subscriptions, facilitating targeted content and bundle offers.

The operation targets the customer_id and subscription_type columns, ensuring that only customers with the same type of subscription (e.g., Monthly/Yearly) in both digital and print formats are selected.

TABLE A
+++++++
@BIGBro: Opening zzzzz_digital_subs.csv

|customer_id |digital_sub_start |type   |
-----------------------------------------
|1           |2024-01-01        |Monthly|
|2           |2024-02-01        |Yearly |
|3           |2024-01-15        |Monthly|
|4           |2024-03-01        |Yearly |
|5           |2024-02-20        |Monthly|
Total rows: 5

TABLE B
+++++++
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_INNER_JOIN_INTERSECTION_WITH B
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 26

|customer_id |print_sub_start |type   |
---------------------------------------
|6           |2024-03-07      |Monthly|
|2           |2024-02-01      |Yearly |
|3           |2024-01-15      |Monthly|
|9           |2024-04-01      |Yearly |
|5           |2024-02-20      |Monthly|
Total rows: 5

  @LILbro: Enter column names (comma separated, if multiple) to SET_INNER_JOIN_INTERSECTION_WITH at: customer_id, type

|customer_id |type   |digital_sub_start |print_sub_start |
----------------------------------------------------------
|2           |Yearly |2024-02-01        |2024-02-01      |
|3           |Monthly|2024-01-15        |2024-01-15      |
|5           |Monthly|2024-02-20        |2024-02-20      |
Total rows: 3
"#,
                );
                return Ok((csv_builder, false));
            }

            print_insight_level_2("Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_INNER_JOIN_INTERSECTION_WITH B");

            let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);

            if let Some(ref chosen_file_path_for_join) = chosen_file_path_for_join {
                let _ = CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                println!();
            }

            if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                // Capture user input for key columns
                let set_intersection_at_choice = get_user_input_level_2(
        "Enter column names (comma separated, if multiple) to SET_INNER_JOIN_INTERSECTION_WITH at: ",
    );

                if handle_cancel_flag(&set_intersection_at_choice) {
                    return Ok((csv_builder, false));
                }

                // Split the input string into a vector of &str, trimming whitespace and ignoring empty entries
                let key_columns: Vec<&str> = set_intersection_at_choice
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect();

                // Ensure that there is at least one key column specified
                if key_columns.is_empty() {
                    println!(
                        "Error: No key columns specified. Please specify at least one key column."
                    );
                } else {
                    // Perform set intersection with the specified key columns
                    csv_builder
                        .set_intersection_with_csv_file(
                            &chosen_file_path_for_join,
                            key_columns,
                            "INTERSECTION_TYPE:INNER_JOIN",
                        )
                        .print_table();
                }
            }
        }

        "8" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

A 'SET DIFFERENCE WITH' analysis is an insightful tool for identifying unique elements in one dataset compared to another, especially when managing data sets with similar column names that serve distinct purposes. This type of analysis becomes particularly valuable in scenarios where we need to highlight differences rather than similarities, such as identifying exclusive customer segments, unique sales transactions, or distinct items sold.

Unlike 'SET INTERSECTION WITH' (which ascertains commonalities between data sets A and B), 'SET DIFFERENCE WITH' helps ascertain the diffierences i.e. elements in A that are not in B.

### Example 1

Background: A retail company operates both an online store and several physical locations. To better understand their customer base, they want to identify customers who shop exclusively online or in-store, as opposed to those who shop in both channels. This information can be crucial for tailoring marketing strategies, such as exclusive offers to entice in-store customers to try online shopping and vice versa.

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
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_DIFFERENCE_WITH B
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 26

|id |sales |date      |
-----------------------
|6  |190   |2024-03-07|
|2  |40    |2024-03-08|
|3  |700   |2024-03-09|
|9  |100   |2024-03-10|
|5  |200   |2024-02-05|
Total rows: 5

  @LILbro: Enter column names (comma separated, if multiple) to SET_DIFFERENCE_WITH at: id

|id |sales |date      |
-----------------------
|6  |190   |2024-03-07|
|9  |100   |2024-03-10|
Total rows: 2

### Example 2

Background: A supermarket chain is assessing the performance of different product lines across their stores. Specifically, they wish to identify products that are sold exclusively in certain stores, which might indicate regional preferences or the impact of local promotions. This can help in optimizing stock levels and tailoring marketing efforts to match local consumer behavior.

1. The operation SET_DIFFERENCE_WITH at 'category, item, purchase_day': This command calculates the difference between TABLE A and TABLE B based on the uniqueness of row values accross all three columns: category, item, and purchase_day. The operation identifies records present in TABLE A but not in TABLE B, highlighting specific products of a specific category, sold on specific days in one store that aren't in the other.
2. The operation SET_DIFFERENCE_WITH at 'category, item': By focusing on just the category and item columns and excluding purchase_day, this comparison identifies specific products of a specific category that are sold exclusively in one store (TABLE A), regardless of the day. This helps to understand core inventory differences and exclusive product offerings between stores.
3. The operation SET_DIFFERENCE_WITH at 'item, purchase_day': This analysis looks at differences based solely on item and purchase_day, omitting category. It serves to pinpoint specific items sold on certain days in one store (TABLE A) but not in the other (TABLE B), regardless of the category, offering insights into day-specific sales patterns and possibly the timing of promotions.

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
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_DIFFERENCE_WITH B
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

  @LILbro: Enter column names (comma separated, if multiple) to SET_DIFFERENCE_WITH at: category, item, pur
chase_day

|category |item  |purchase_day |
--------------------------------
|Dairy    |Cheese|Wednesday    |
|Snacks   |Chips |Friday       |
|Beverages|Coffee|Monday       |
Total rows: 3

  @LILbro: Enter column names (comma separated, if multiple) to SET_DIFFERENCE_WITH at: cate
gory, item

|category |item  |purchase_day |
--------------------------------
|Dairy    |Cheese|Wednesday    |
|Snacks   |Chips |Friday       |
Total rows: 2

  @LILbro: Enter column names (comma separated, if multiple) to SET_DIFFERENCE_WITH at: item
, purchase_day

|category |item  |purchase_day |
--------------------------------
|Dairy    |Cheese|Wednesday    |
|Snacks   |Chips |Friday       |
|Beverages|Coffee|Monday       |
Total rows: 3
"#,
                );
                return Ok((csv_builder, false));
            }

            print_insight_level_2("Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_DIFFERENCE_WITH B");

            let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);

            if let Some(ref chosen_file_path_for_join) = chosen_file_path_for_join {
                let _ = CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                println!();
            }

            if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                // Capture user input for key columns
                let set_intersection_at_choice = get_user_input_level_2(
                    "Enter column names (comma separated, if multiple) to SET_DIFFERENCE_WITH at: ",
                );

                if handle_cancel_flag(&set_intersection_at_choice) {
                    return Ok((csv_builder, false));
                }

                // Split the input string into a vector of &str, trimming whitespace and ignoring empty entries
                let key_columns: Vec<&str> = set_intersection_at_choice
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect();

                // Ensure that there is at least one key column specified
                if key_columns.is_empty() {
                    println!(
                        "Error: No key columns specified. Please specify at least one key column."
                    );
                } else {
                    // Perform set intersection with the specified key columns
                    csv_builder
                        .set_difference_with_csv_file(
                            &chosen_file_path_for_join,
                            "DIFFERENCE_TYPE:NORMAL",
                            key_columns,
                        )
                        .print_table();
                }
            }
        }

        "9" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

A 'SET SYMMETRIC DIFFERENCE WITH' analysis is a powerful technique for identifying elements that are unique to each of two datasets, essentially highlighting the differences between them without overlap. This method is invaluable when the objective is to uncover exclusive elements in both sets, thereby providing a comprehensive view of unique attributes, transactions, or records that do not have a common counterpart in the compared datasets.

Unlike 'SET DIFFERENCE WITH' (which identifies elements present in one dataset but not in the other) or 'SET INTERSECTION WITH' (which finds common elements between datasets), 'SET SYMMETRIC DIFFERENCE WITH' reveals elements that are unique to each dataset, offering insights into distinctive characteristics that define each set independently.

### Example 1

Background: A retail company operates both an online store and several physical locations. To better understand their customer base, they want to identify unique customer segments who either shop exclusively online or in-store, using customer IDs to track shopping behavior. 

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
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_SYMMETRIC_DIFFERENCE_WITH B
  @LILbro: Punch in the serial number or a slice of the file name to LOAD: 26

|id |sales |date      |
-----------------------
|6  |190   |2024-03-07|
|2  |40    |2024-03-08|
|3  |700   |2024-03-09|
|9  |100   |2024-03-10|
|5  |200   |2024-02-05|
Total rows: 5

  @LILbro: Enter column names (comma separated, if multiple) to SET_SYMMETRIC_DIFFERENCE_WITH at: id

|id |sales |date      |
-----------------------
|6  |190   |2024-03-07|
|9  |100   |2024-03-10|
|1  |120   |2024-03-01|
|4  |500   |2024-03-04|
Total rows: 4

### Example 2

Background: A supermarket chain is assessing the performance of different product lines across their stores. Specifically, they wish to identify products that are only sold via Store A (which as a particular decor/ set up), and also, products that are only sold via Store B (which has a particular decor/ set up) - excluding those products that are purchases across both channels.

1. The operation SET_SYMMETRIC_DIFFERENCE_WITH at 'category, item, purchase_day': Assesses the exclusivity of products based on their category, item, and purchase_day across both stores. It identifies unique products sold in Store A not found in Store B and vice versa, based on the specificity of category, item, and the day of purchase. This operation helps pinpoint exclusive product sales trends and preferences unique to each store setup on specific days.

2. The operation SET_SYMMETRIC_DIFFERENCE_WITH at 'category, item': Focusing on category and item alone, this operation reveals the core differences in product lineups between Store A and Store B, disregarding the purchase day. It identifies unique categories and items that are exclusively sold in either store, underscoring the distinctive inventory and product offerings that cater to the particular tastes and preferences of their respective customer bases.

3. The operation SET_SYMMETRIC_DIFFERENCE_WITH at 'item, purchase_day': By analyzing only the item and purchase_day, this comparison sheds light on unique items sold on specific days in either store, omitting the category context. It serves to highlight day-specific sales trends and potentially exclusive promotional activities for certain items, offering insights into the timing and exclusivity of product offerings between the two store types.

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
  @LILBro: Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_SYMMETRIC_DIFFERENCE_WITH B
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

  @LILbro: Enter column names (comma separated, if multiple) to SET_SYMMETRIC_DIFFERENCE_WITH at: category, item, purchase_day

|category |item  |purchase_day |
--------------------------------
|Dairy    |Cheese|Wednesday    |
|Snacks   |Chips |Friday       |
|Beverages|Coffee|Monday       |
|Dairy    |Butter|Wednesday    |
|Snacks   |Nuts  |Friday       |
|Beverages|Tea   |Friday       |
Total rows: 6

  @LILbro: Enter column names (comma separated, if multiple) to SET_SYMMETRIC_DIFFERENCE_WITH at: category, 
item

|category |item  |purchase_day |
--------------------------------
|Dairy    |Cheese|Wednesday    |
|Snacks   |Chips |Friday       |
|Dairy    |Butter|Wednesday    |
|Snacks   |Nuts  |Friday       |
Total rows: 4

  @LILbro: Enter column names (comma separated, if multiple) to SET_SYMMETRIC_DIFFERENCE_WITH at: item, purc
hase_day

|category |item  |purchase_day |
--------------------------------
|Dairy    |Cheese|Wednesday    |
|Snacks   |Chips |Friday       |
|Beverages|Coffee|Monday       |
|Dairy    |Butter|Wednesday    |
|Snacks   |Nuts  |Friday       |
|Beverages|Tea   |Friday       |
Total rows: 6
"#,
                );
                return Ok((csv_builder, false));
            }

            print_insight_level_2("Your current csv is the 'A Table'. Now, choose the 'B Table' for the operation A SET_SYMMETRIC_DIFFERENCE_WITH B");

            let chosen_file_path_for_join = select_csv_file_path(&csv_db_path_buf);

            if let Some(ref chosen_file_path_for_join) = chosen_file_path_for_join {
                let _ = CsvBuilder::from_csv(&chosen_file_path_for_join).print_table();
                println!();
            }

            if let Some(chosen_file_path_for_join) = chosen_file_path_for_join {
                // Capture user input for key columns
                let set_intersection_at_choice = get_user_input_level_2(
        "Enter column names (comma separated, if multiple) to SET_SYMMETRIC_DIFFERENCE_WITH at: ",
    );

                if handle_cancel_flag(&set_intersection_at_choice) {
                    return Ok((csv_builder, false));
                }

                // Split the input string into a vector of &str, trimming whitespace and ignoring empty entries
                let key_columns: Vec<&str> = set_intersection_at_choice
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect();

                // Ensure that there is at least one key column specified
                if key_columns.is_empty() {
                    println!(
                        "Error: No key columns specified. Please specify at least one key column."
                    );
                    return Ok((csv_builder, false));
                } else {
                    // Perform set intersection with the specified key columns
                    csv_builder
                        .set_difference_with_csv_file(
                            &chosen_file_path_for_join,
                            "DIFFERENCE_TYPE:SYMMETRIC",
                            key_columns,
                        )
                        .print_table();
                }
            }
        }

        _ => {
            println!("Invalid option. Please enter a number from 1 to 9.");
            return Ok((csv_builder, false));
        }
    }

    println!();
    return Ok((csv_builder, true));
}
