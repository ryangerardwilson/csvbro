// src/user_experience.rs
use crate::config::edit_config;
use crate::csv_manager::{delete_csv_file, import, open_csv_file};
use crate::db_connector::query;
use crate::user_interaction::{get_user_input, get_user_input_level_2, print_insight, print_list};
use rgwml::csv_utils::CsvBuilder;
use std::env;
use std::path::Path;
use std::path::PathBuf;

pub fn handle_special_flag_without_builder(flag: &str) -> bool {
    let home_dir = env::var("HOME").expect("Unable to determine user home directory");
    let desktop_path = Path::new(&home_dir).join("Desktop");
    let csv_db_path = desktop_path.join("csv_db");

    let csv_db_path_buf = PathBuf::from(csv_db_path.clone());

    match flag {
        "@f" | "@flags " => {
            let flags = vec![
                "@c           : After action select/ in vim edit => Cancel action",
                "@config      : Primary/ Secondary menu => Edit config",
                "@d / @delete : Primary/ Secondary menu => Delete files from csv_db",
                "@f/ @flags   : Primary/ Secondary menu => View all flags",
                "@i / @import : After csv load => Import a new csv file",
                "@n / @new    : After csv load => Instantiate a new blank csv file",
                "@o / @open   : After csv load => Open a new csv file from csv_db",
                "@query       : After csv load => Import a new csv via a db query",
                "@r           : After csv load via query => Retry a query",
                "@s           : After csv load => Save",
                "@sa          : After csv load => Save as",
                "@q           : Anywhere => Quit csvbro",
            ];

            print_insight("Serving your flags ...");
            print_list(&flags);
            println!();
            true
        }
        "@d" => {
            delete_csv_file(&csv_db_path_buf);
            true
        }
        "@config" => {
            let _ = edit_config(&csv_db_path_buf);
            true
        }

        _ => false,
    }
}

pub fn handle_special_flag(
    flag: &str,
    builder: &mut CsvBuilder,
    file_path_option: Option<&str>,
) -> bool {
    let current_file_path: Option<PathBuf> = file_path_option.map(PathBuf::from);
    let has_data = builder.has_data();

    let home_dir = env::var("HOME").expect("Unable to determine user home directory");
    let desktop_path = Path::new(&home_dir).join("Desktop");
    let csv_db_path = desktop_path.join("csv_db");

    //let csv_db_path_buf = PathBuf::from(csv_db_path.clone());

    match flag {
        "@s" => {
            // Perform action for "@s"

            if has_data {
                if let Some(ref path) = current_file_path {
                    // Save to the existing file path
                    let _ = builder.save_as(path.to_str().unwrap());
                    println!();
                    print_insight(&format!("CSV file saved at {}\n", path.display()));
                } else {
                    let file_name =
                        get_user_input_level_2("Enter file name to save (without extension): ");
                    let full_file_name = if file_name.ends_with(".csv") {
                        file_name
                    } else {
                        format!("{}.csv", file_name)
                    };
                    let file_path = csv_db_path.join(full_file_name);
                    let _ = builder.save_as(file_path.to_str().unwrap());
                    print_insight(&format!("CSV file saved at {}", file_path.display()));
                }
            }

            true
        }
        "@sa" => {
            // Perform action for "@sa"

            if has_data {
                println!();
                let file_name = get_user_input("Enter file name to save (without extension): ");
                let full_file_name = if file_name.ends_with(".csv") {
                    file_name
                } else {
                    format!("{}.csv", file_name)
                };
                let file_path = csv_db_path.join(full_file_name);
                let _ = builder.save_as(file_path.to_str().unwrap());
                println!();
                print_insight(&format!("CSV file saved at {}\n", file_path.display()));
                //break; // Exit the loop after saving
            }

            true
        }
        _ => false,
    }
}

pub async fn handle_special_flag_returning_new_builder(
    flag: &str,
) -> Option<Result<((), CsvBuilder), Box<dyn std::error::Error>>> {
    let home_dir = match env::var("HOME") {
        Ok(dir) => dir,
        Err(_) => {
            println!("Unable to determine user home directory");
            return None;
        }
    };
    let desktop_path = Path::new(&home_dir).join("Desktop");
    let downloads_path = Path::new(&home_dir).join("Downloads");
    let csv_db_path = desktop_path.join("csv_db");
    let csv_db_path_buf = PathBuf::from(&csv_db_path);

    let mut new_builder = CsvBuilder::new();

    match flag {
        "@n" | "@new" => {
            let file_name = get_user_input("Enter file name to save (without extension): ");
            let full_file_name = if file_name.ends_with(".csv") {
                file_name
            } else {
                format!("{}.csv", file_name)
            };
            let file_path = csv_db_path.join(full_file_name);
            let file_path_str = file_path.to_str()?;

            if let Err(e) = new_builder.save_as(file_path_str) {
                //return Some(Err(Box::new(e)));
                return Some(Err(e));
            }
        }
        "@o" | "@open" => {
            if let Some((opened_builder, _file_path)) = open_csv_file(&csv_db_path_buf) {
                return Some(Ok(((), opened_builder)));
            } else {
                println!("Error: Could not open the specified CSV file");
            }
        }
        "@i" | "@import" => {
            let desktop_path_buf = PathBuf::from(&desktop_path);
            let downloads_path_buf = PathBuf::from(&downloads_path);
            if let Some(imported_builder) = import(&desktop_path_buf, &downloads_path_buf) {
                return Some(Ok(((), imported_builder)));
            } else {
                println!("Error: Could not import the CSV file.");
            }
        }
        "@query" => {
            if let Err(e) = query(&csv_db_path_buf).await {
                println!("Query failed: {}", e);
            }
        }
        _ => {
            //new_builder = CsvBuilder::from_copy(builder);
            //return Some(Ok(((), new_builder)));
            return None;
        }
    }

    Some(Ok(((), new_builder)))
}

pub fn handle_query_special_flag(flag: &str, builder: &mut CsvBuilder) -> bool {
    let has_data = builder.has_data();

    let home_dir = env::var("HOME").expect("Unable to determine user home directory");
    let desktop_path = Path::new(&home_dir).join("Desktop");
    let csv_db_path = desktop_path.join("csv_db");

    match flag {
        "@s" | "@sa" => {
            // Perform action for "@sa"

            if has_data {
                println!();
                let file_name = get_user_input("Enter file name to save (without extension): ");
                let full_file_name = if file_name.ends_with(".csv") {
                    file_name
                } else {
                    format!("{}.csv", file_name)
                };
                let file_path = csv_db_path.join(full_file_name);
                let _ = builder.save_as(file_path.to_str().unwrap());
                println!();
                print_insight(&format!("CSV file saved at {}\n", file_path.display()));
                //break; // Exit the loop after saving
            }

            true
        }
        _ => false,
    }
}

pub fn handle_query_retry_flag(flag: &str) -> bool {
    match flag {
        "@r" => true,
        _ => false,
    }
}

pub fn handle_back_flag(flag: &str) -> bool {
    match flag {
        "@b" => true,
        _ => false,
    }
}

pub fn handle_quit_flag(flag: &str) {
    if flag == "@q" {
        std::process::exit(0);
    }
}

pub fn handle_cancel_flag(flag: &str) -> bool {
    let trimmed = flag.trim();
    match trimmed {
        f if f == "@c" => true,
        f if f.starts_with("@c") => true,
        _ => false,
    }
}
