// src/user_experience.rs
use crate::user_interaction::{get_user_input, get_user_input_level_2, print_insight};
use rgwml::csv_utils::CsvBuilder;
use std::env;
use std::path::Path;
use std::path::PathBuf;

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
    match flag {
        "@c" => true,
        _ => false,
    }
}
