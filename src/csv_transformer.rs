// csv_transformer.rs
use crate::user_experience::handle_cancel_flag;
use crate::user_interaction::{
    get_edited_user_json_input, get_user_input_level_2, print_insight_level_2, print_list_level_2,
};
use rgwml::csv_utils::{CsvBuilder, Piv};
use serde_json::Value;
use std::fs;

pub async fn handle_transform(
    mut csv_builder: CsvBuilder,
    _file_path_option: Option<&str>,
    action_type: &str,
    action_feature: &str,
    action_flag: &str,
    action_menu_options: Vec<&str>,
) -> Result<(CsvBuilder, bool), Box<dyn std::error::Error>> {
    fn get_pivot_input() -> Result<Piv, Box<dyn std::error::Error>> {
        let pivot_syntax = r#"{
    "index_at": "",
    "values_from": "",
    "operation": "",
    "seggregate_by": [
        {"column": "", "type": ""}
    ]
}

SYNTAX
======

Unlike a broad grouping, a pivot is grouping that emphasizes aggregating numerical values, and segregating those aggregates.

{
    "index_at": "Date",      // Name of the column to index/ group by
    "values_from": "Sales",
    "operation": "MEDIAN", // Also "COUNT", "COUNT_UNIQUE", "NUMERICAL_MIN", "NUMERICAL_MAX", "NUMERICAL_SUM", "NUMERICAL_MEAN", "NUMERICAL_MEDIAN", "NUMERICAL_STANDARD_DEVIATION", "BOOL_PERCENT" (assuming column values of 0 or 1 in 'values_from', calculates the % of 1 values for the segment)
    "seggregate_by": [  // Leave as empty [] if seggregation is not required
        {"column": "Category", "type": "AS_CATEGORY"},
        {"column": "IsPromotion", "type": "AS_BOOLEAN"}
    ],
}

Note the implication of params in the Json Query:
1. "index_at": This parameter determines the primary key column of the pivot table, or the field by which the data will be grouped vertically (row labels). It's the main dimension of analysis. This can be either a text or a number, depending on the data you are grouping by. For example, if you are grouping sales data by region, index_at could be the name of the region (text). If you are grouping by year, it could be the year (number).
2. "values_from": Specifies the column(s) from which to retrieve the values that will be summarized or aggregated in the pivot table. This would be a column with numerical data since you are usually performing operations like sums, averages, counts, etc.
3. "operation": Defines the type of aggregation or summarization to perform on the values_from data across the grouped index_at categories. These include:

 - `COUNT_UNIQUE` - Counts the unique values in the column.
 - `NUMERICAL_MAX` - Finds the maximum numerical value in the column.
 - `NUMERICAL_MIN` - Finds the minimum numerical value in the column.
 - `NUMERICAL_SUM` - Calculates the sum of numerical values in the column.
 - `NUMERICAL_MEAN` - Calculates the mean (average) of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_MEDIAN` - Calculates the median of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_STANDARD_DEVIATION` - Calculates the standard deviation of numerical values in the column, rounded to two decimal places.
 - `BOOL_PERCENT` - Calculates the percentage of `1`s in the column, assuming the values are either `1` or `0`, rounded to two decimal places.

4. "seggregate_by": This parameter allows for additional segmentation of data within the primary grouping defined by index_at. Each segment within seggregate_by can further divide the data based on the specified column and the type of segmentation (like categorical grouping or binning numerical data into ranges).
- 4.1. Column: Can be both text or number, similar to index_at, depending on what additional dimension you want to segment the data by.
- 4.2. Type: Is text, indicating how the segmentation should be applied. The column specified can have a type of "AS_CATEGORY", or "AS_BOOLEAN"
  - 4.2.1. AS_CATEGORY: It means that each unique value in the specified seggregation column will create a separate subgroup within each primary group. This is appropriate for text data or numerical data that represent distinct categories or groups rather than values to be aggregated.
  - 4.2.2. AS_BOOLEAN: By setting the type to "AS_BOOLEAN", it's understood that the specified seggregation column contains boolean values (1/0). The data will be segmented into two groups based on these boolean values. This type is particularly useful for flag columns that indicate the presence or absence of a particular condition or attribute.
"#;

        let user_input = get_edited_user_json_input(pivot_syntax.to_string());

        if handle_cancel_flag(&user_input) {
            return Err("Operation canceled".into());
        }

        let parsed_json: Value = serde_json::from_str(&user_input)?;

        let index_at = parsed_json["index_at"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let values_from = parsed_json["values_from"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let operation = parsed_json["operation"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let seggregate_by: Vec<(String, String)> = parsed_json["seggregate_by"]
            .as_array()
            .map_or_else(Vec::new, |items| {
                items
                    .iter()
                    .filter_map(|item| {
                        let column = item["column"].as_str().unwrap_or("").to_string();
                        let type_ = item["type"].as_str().unwrap_or("").to_string();
                        if column.is_empty() || type_.is_empty() {
                            None // Exclude items where either column or type is empty
                        } else {
                            Some((column, type_)) // Include valid segregation criteria
                        }
                    })
                    .collect()
            });

        Ok(Piv {
            index_at,
            values_from,
            operation,
            seggregate_by,
        })
    }

    match action_feature {
        "" => {
            let action_sub_menu_options = vec!["TRANSPOSE", "GROUP", "GROUPED SPLIT", "PIVOT"];
            print_list_level_2(&action_menu_options, &action_sub_menu_options, &action_type);

            return Ok((csv_builder, false));
        }

        "1" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

"#,
                );
                return Ok((csv_builder, false));
            }

            csv_builder.transpose_transform();
            if csv_builder.has_data() {
                csv_builder.print_table();
                println!();
            }
        }

        "2" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Groups a log table by a particular column, transmuting all data pertaining to unique values of that column into a ordered array stored in a grouped column.

|id |item      |calorie_count |
-------------------------------
|1  |pizza     |500           |
|2  |milk shake|300           |
|3  |potatoe   |100           |
|4  |pizza     |600           |
Total rows: 4

  @LILbro: Executing this JSON query:
{
  "group_by_column": "item",
  "grouped_column_name": "history",
  "feature_flags": {
    "id": "",
    "item": "",
    "calorie_count": ["NUMERICAL_MAX"]
  }
}

|item      |history                                      |history_count |history_unique_count |calorie_count_numerical_max |
----------------------------------------------------------------------------------------------------------------------------
|milk shake|[{"calorie_count":"300","id":"2","item":"milk|1             |1                    |300                         |
|pizza     |[{"calorie_count":"500","id":"1","item":"pizz|2             |2                    |600                         |
|potatoe   |[{"calorie_count":"100","id":"3","item":"pota|1             |1                    |100                         |
Total rows: 3

# Available Feature Flahs

The following feature flags can be used to perform different types of calculations on the specified columns:
 - `COUNT_UNIQUE` - Counts the unique values in the column.
 - `NUMERICAL_MAX` - Finds the maximum numerical value in the column.
 - `NUMERICAL_MIN` - Finds the minimum numerical value in the column.
 - `NUMERICAL_SUM` - Calculates the sum of numerical values in the column.
 - `NUMERICAL_MEAN` - Calculates the mean (average) of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_MEDIAN` - Calculates the median of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_STANDARD_DEVIATION` - Calculates the standard deviation of numerical values in the column, rounded to two decimal places.
 - `DATETIME_MAX` - Finds the maximum datetime value in the column, based on specified formats.
 - `DATETIME_MIN` - Finds the minimum datetime value in the column, based on specified formats.
 - `DATETIME_COMMA_SEPARATED` - Comma separates datetime values in the columns
- `MODE` - Finds the most frequent value in the column.
 - `BOOL_PERCENT` - Calculates the percentage of `1`s in the column, assuming the values are either `1` or `0`, rounded to two decimal places.
"#,
                );
                return Ok((csv_builder, false));
            }

            if let Some(headers) = csv_builder.get_headers() {
                let mut json_str = "{\n".to_string();

                json_str.push_str(&format!("  \"group_by_column\": \"\",\n"));
                json_str.push_str(&format!("  \"grouped_column_name\": \"\",\n"));

                json_str.push_str("  \"feature_flags\": {\n");

                for (i, header) in headers.iter().enumerate() {
                    json_str.push_str(&format!("    \"{}\": [\"\"]", header));
                    if i < headers.len() - 1 {
                        json_str.push_str(",\n");
                    }
                }

                json_str.push_str("\n  }\n");

                json_str.push_str("}");

                let syntax = r#"

SYNTAX
======

{
  "group_by_column": "item",                              // The column to GROUP BY with
  "grouped_column_name": "history",                       // The name of the column compressing all row data
  "feature_flags": {
    "id": [""],
    "item": [""],                                         // Leave blank to exclude
    "calorie_count": ["NUMERICAL_MAX", "NUMERICAL_MEAN"]  // Specify a feature flag
  }
}

# Available Feature Flags

The following feature flags can be used to perform different types of calculations on the specified columns:
 - `COUNT_UNIQUE` - Counts the unique values in the column.
 - `NUMERICAL_MAX` - Finds the maximum numerical value in the column.
 - `NUMERICAL_MIN` - Finds the minimum numerical value in the column.
 - `NUMERICAL_SUM` - Calculates the sum of numerical values in the column.
 - `NUMERICAL_MEAN` - Calculates the mean (average) of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_MEDIAN` - Calculates the median of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_STANDARD_DEVIATION` - Calculates the standard deviation of numerical values in the column, rounded to two decimal places.
 - `DATETIME_MAX` - Finds the maximum datetime value in the column, based on specified formats.
 - `DATETIME_MIN` - Finds the minimum datetime value in the column, based on specified formats.
 - `DATETIME_COMMA_SEPARATED` - Comma separates datetime values in the columns
 - `MODE` - Finds the most frequent value in the column.
 - `BOOL_PERCENT` - Calculates the percentage of `1`s in the column, assuming the values are either `1` or `0`, rounded to two decimal places.
                "#;

                json_str.push_str(syntax);

                let row_json_str = json_str;

                let edited_json = get_edited_user_json_input(row_json_str);

                let edited_value: Value =
                    serde_json::from_str(&edited_json).expect("Invalid JSON format");

                // Extract values from the edited JSON
                let group_by_column = edited_value["group_by_column"]
                    .as_str()
                    .expect("group_by_column is not a string");
                let grouped_column_name = edited_value["grouped_column_name"]
                    .as_str()
                    .expect("grouped_column_name is not a string");

                let mut feature_flags: Vec<(String, String)> = Vec::new();
                if let Value::Object(map) = &edited_value["feature_flags"] {
                    for (key, value) in map.iter() {
                        if let Value::Array(arr) = value {
                            for val in arr {
                                if let Value::String(feature_flag) = val {
                                    if !feature_flag.is_empty() {
                                        feature_flags.push((key.clone(), feature_flag.clone()));
                                    }
                                }
                            }
                        }
                    }
                }

                // dbg!(&group_by_column, &grouped_column_name, &feature_flags);

                csv_builder.grouped_index_transform(
                    &group_by_column,
                    &grouped_column_name,
                    feature_flags,
                );
            }

            if csv_builder.has_data() {
                csv_builder.print_table();
                println!();
            }
        }

        "3" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Generates csv files at a specified directory, splitting your current files based on a GROUP BY column.

|id |item      |calorie_count |
-------------------------------
|1  |pizza     |500           |
|2  |milk shake|300           |
|3  |potatoe   |100           |
|4  |pizza     |600           |
Total rows: 4

  @LILbro: Enter the column name to group the data by: item
  @LILbro: Enter file path of directory to store grouped data: /home/rgw/Desktop/split_csvs_dr
  @LILBro: Split completed at /home/rgw/Desktop/split_csvs_dr. 3 files generated!

/// In the concerned directory:

    rgw@rgw-asus:~/Desktop/split_csvs_dr$ ls
        'group_split_by_milk shake_in_item.csv'   
        group_split_by_potatoe_in_item.csv
        group_split_by_pizza_in_item.csv
"#,
                );
                return Ok((csv_builder, false));
            }

            let group_by_column_name_str =
                get_user_input_level_2("Enter the column name to group the data by: ");

            if handle_cancel_flag(&group_by_column_name_str) {
                return Ok((csv_builder, false));
            }

            let grouped_data_dir_path_str =
                get_user_input_level_2("Enter file path of directory to store grouped data: ");

            if handle_cancel_flag(&grouped_data_dir_path_str) {
                return Ok((csv_builder, false));
            }

            let _ = csv_builder.split_as(&group_by_column_name_str, &grouped_data_dir_path_str);

            let paths = fs::read_dir(grouped_data_dir_path_str.clone()).unwrap();
            let file_count = paths.count();
            let insight = format!(
                "Split completed at {}. {} files generated!",
                grouped_data_dir_path_str, file_count
            );
            print_insight_level_2(&insight);
            println!();
            return Ok((csv_builder, false));
        }

        "4" => {
            if action_flag == "d" {
                print_insight_level_2(
                    r#"DOCUMENTATION

Unlike a broad grouping, a pivot is grouping that emphasizes aggregating numerical values, and segregating those aggregates. This feature creates a pivot table indexed/ grouped at a category label (accruing from the unique values in a category column).

Example 1:
----------
  @LILbro: Executing this JSON query:
{
    "index_at": "date_YEAR_MONTH",
    "values_from": "value",
    "operation": "SUM",
    "seggregate_by": [],
    "save_as": ""
}

|id |item    |value |type  |  <<+2 cols>>  |date_YEAR |date_YEAR_MONTH |date_YEAR_MONTH_DAY |
---------------------------------------------------------------------------------------------
|1  |books   |1000  |small |...            |Y2024     |Y2024-M01       |Y2024-M01-D21       |
|2  |snacks  |200   |small |...            |Y2024     |Y2024-M02       |Y2024-M02-D22       |
|3  |cab fare|300   |small |...            |Y2024     |Y2024-M03       |Y2024-M03-D23       |
|4  |rent    |20000 |big   |...            |Y2024     |Y2024-M01       |Y2024-M01-D24       |
|5  |movies  |1500  |medium|...            |Y2024     |Y2024-M02       |Y2024-M02-D25       |
|6  |books   |1000  |small |...            |Y2024     |Y2024-M03       |Y2024-M03-D21       |
|7  |snacks  |200   |small |...            |Y2024     |Y2024-M01       |Y2024-M01-D22       |
|8  |cab fare|300   |small |...            |Y2024     |Y2024-M02       |Y2024-M02-D23       |
|9  |rent    |20000 |big   |...            |Y2024     |Y2024-M03       |Y2024-M03-D24       |
|10 |movies  |1500  |medium|...            |Y2024     |Y2024-M01       |Y2024-M01-D25       |

Omitted columns: item_type, date
Total rows: 10


|Index    |Value   |
--------------------
|Y2024-M01|22700.00|
|Y2024-M02|2000.00 |
|Y2024-M03|21300.00|
Total rows: 3
Temporary file deleted successfully.

Example 2:
----------
  @LILbro: Executing this JSON query:
{
    "index_at": "date_YEAR_MONTH",
    "values_from": "value",
    "operation": "SUM",
    "seggregate_by": [
        {"column": "relates_to_travel", "type": "AS_BOOLEAN"},
        {"column": "type", "type": "AS_CATEGORY"}
    ],
    "save_as": ""
}

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


|Index    |relates_to_travel |FOOD  |OTHER   |TRAVEL |relates_to_travel |Value   |
----------------------------------------------------------------------------------
|Y2024-M01|0.00              |200.00|22500.00|0.00   |0.00              |22700.00|
|Y2024-M02|300.00            |200.00|1500.00 |300.00 |300.00            |2000.00 |
|Y2024-M03|300.00            |0.00  |21000.00|300.00 |300.00            |21300.00|
Total rows: 3
Temporary file deleted successfully.

Note the implication of params in the Json Query:
1. "index_at": This parameter determines the primary key column of the pivot table, or the field by which the data will be grouped vertically (row labels). It's the main dimension of analysis. This can be either a text or a number, depending on the data you are grouping by. For example, if you are grouping sales data by region, index_at could be the name of the region (text). If you are grouping by year, it could be the year (number).
2. "values_from": Specifies the column(s) from which to retrieve the values that will be summarized or aggregated in the pivot table. This would be a column with numerical data since you are usually performing operations like sums, averages, counts, etc.
3. "operation": Defines the type of aggregation or summarization to perform on the values_from data across the grouped index_at categories. These include:

 - `COUNT_UNIQUE` - Counts the unique values in the column.
 - `NUMERICAL_MAX` - Finds the maximum numerical value in the column.
 - `NUMERICAL_MIN` - Finds the minimum numerical value in the column.
 - `NUMERICAL_SUM` - Calculates the sum of numerical values in the column.
 - `NUMERICAL_MEAN` - Calculates the mean (average) of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_MEDIAN` - Calculates the median of numerical values in the column, rounded to two decimal places.
 - `NUMERICAL_STANDARD_DEVIATION` - Calculates the standard deviation of numerical values in the column, rounded to two decimal places.
 - `BOOL_PERCENT` - Calculates the percentage of `1`s in the column, assuming the values are either `1` or `0`, rounded to two decimal places.

4. "seggregate_by": This parameter allows for additional segmentation of data within the primary grouping defined by index_at. Each segment within seggregate_by can further divide the data based on the specified column and the type of segmentation (like categorical grouping or binning numerical data into ranges).
- 4.1. Column: Can be both text or number, similar to index_at, depending on what additional dimension you want to segment the data by.
- 4.2. Type: Is text, indicating how the segmentation should be applied. The column specified can have a type of "AS_CATEGORY", or "AS_BOOLEAN"
  - 4.2.1. AS_CATEGORY: It means that each unique value in the specified seggregation column will create a separate subgroup within each primary group. This is appropriate for text data or numerical data that represent distinct categories or groups rather than values to be aggregated.
  - 4.2.2. AS_BOOLEAN: By setting the type to "AS_BOOLEAN", it's understood that the specified seggregation column contains boolean values (1/0). The data will be segmented into two groups based on these boolean values. This type is particularly useful for flag columns that indicate the presence or absence of a particular condition or attribute.
"#,
                );
                return Ok((csv_builder, false));
            }

            // This matches the case in your project's workflow for the pivot operation
            match get_pivot_input() {
                Ok(piv) => {
                    csv_builder.pivot_as(piv).print_table();
                    println!();
                }
                Err(e) if e.to_string() == "Operation canceled" => {
                    return Ok((csv_builder, false));
                }

                Err(e) => {
                    println!("Error getting pivot details: {}", e);
                    return Ok((csv_builder, false));
                }
            }
        }

        _ => {
            println!("Invalid option. Please enter a number from 1 to 3.");
            return Ok((csv_builder, false));
        }
    }

    return Ok((csv_builder, true));
}
