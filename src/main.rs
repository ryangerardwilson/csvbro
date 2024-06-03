mod config;
mod csv_appender;
mod csv_inspector;
mod csv_joiner;
mod csv_manager;
mod csv_predicter;
mod csv_searcher;
mod csv_tinkerer;
mod csv_transformer;
mod db_connector;
mod user_experience;
mod user_interaction;

use crate::config::edit_config;
use crate::csv_manager::{chain_builder, delete_csv_file, import, import_from_url, open_csv_file};
use crate::db_connector::query;
use crate::user_experience::{handle_quit_flag, handle_special_flag_without_builder};
use crate::user_interaction::{
    determine_action_as_text, get_user_input, print_insight, print_list,
};
use rgwml::csv_utils::CsvBuilder;
use std::env;
use std::path::Path;
use std::path::PathBuf;
use std::process::{self, Command};

const BRO_VERSION: &str = "1.3.2";

#[tokio::main]
async fn main() {
    fn embed_and_set_up_in_directory_system(
    ) -> Result<(String, String, String), Box<dyn std::error::Error>> {
        // Attempt to dynamically find the current executable's path
        let current_exe_path_buf =
            env::current_exe().expect("Failed to find current executable path");
        let current_exe_path = current_exe_path_buf.as_path();

        let home_dir = env::var("HOME").expect("Unable to determine user home directory");
        let desktop_path = Path::new(&home_dir).join("Desktop");
        let downloads_path = Path::new(&home_dir).join("Downloads");
        let csv_db_path = desktop_path.join("csv_db");

        // Define the target path for moving the binary
        let target_binary_path = Path::new("/usr/local/bin/csvbro");

        // Deduce if running via cargo by checking if the executable path contains target/debug or target/release
        let is_cargo_run = current_exe_path
            .to_string_lossy()
            .contains("/target/debug/")
            || current_exe_path
                .to_string_lossy()
                .contains("/target/release/");

        // Check if the binary is being executed from the target path
        let is_executed_from_target = current_exe_path == target_binary_path;

        if !is_executed_from_target && !is_cargo_run {
            // Install pip dependencies to bare metal
            let packages: Vec<(&str, &str)> = vec![
                ("google-cloud-bigquery", "google.cloud.bigquery"),
                ("clickhouse-driver", "clickhouse_driver"),
                ("pandas", "pandas"),
                ("xgboost", "xgboost"),
                ("scikit-learn", "sklearn"),
                ("numpy", "numpy"),
            ];

            let mut missing_packages = Vec::new();

            for (package_name, import_name) in &packages {
                // Check if the package can be imported
                let check_package_status = Command::new("python3")
                    .arg("-c")
                    .arg(format!("import {}", import_name))
                    .status();

                // If the package is missing, add it to the list of missing packages
                if check_package_status.is_err() || !check_package_status.unwrap().success() {
                    missing_packages.push(*package_name);
                }
            }

            // Install missing packages if any
            if !missing_packages.is_empty() {
                let pip_install_status = Command::new("pip3")
                    .args(&["install"])
                    .args(&missing_packages)
                    .status()?;
                if !pip_install_status.success() {
                    return Err("Failed to install packages".into());
                }
            }

            // Move the binary to the target path using 'sudo mv'
            let status = Command::new("sudo")
                .arg("mv")
                .arg(current_exe_path)
                .arg(target_binary_path)
                .status()
                .expect("Failed to execute process");

            if !status.success() {
                eprintln!("Failed to move binary to /usr/local/bin. You may be prompted for your password.");
                process::exit(1);
            }
        }

        Ok((
            desktop_path.to_string_lossy().into_owned(),
            downloads_path.to_string_lossy().into_owned(),
            csv_db_path.to_string_lossy().into_owned(),
        ))
    }

    if std::env::args().any(|arg| arg == "--version") {
        print_insight(BRO_VERSION);
        std::process::exit(0);
    }

    //let (desktop_path, downloads_path, csv_db_path) = embed_and_set_up_in_directory_system();
    let (desktop_path, downloads_path, csv_db_path) =
        embed_and_set_up_in_directory_system().expect("Failed to set up directory system");

    let csv_db_path_buf = PathBuf::from(csv_db_path);
    let desktop_path_buf = PathBuf::from(desktop_path);
    let downloads_path_buf = PathBuf::from(downloads_path);

    println!(
        r#"

          _____                    _____                    _____                            _____                    _____                   _______         
         /\    \                  /\    \                  /\    \                          /\    \                  /\    \                 /::\    \        
        /::\    \                /::\    \                /::\____\                        /::\    \                /::\    \               /::::\    \       
       /::::\    \              /::::\    \              /:::/    /                       /::::\    \              /::::\    \             /::::::\    \      
      /::::::\    \            /::::::\    \            /:::/    /                       /::::::\    \            /::::::\    \           /::::::::\    \     
     /:::/\:::\    \          /:::/\:::\    \          /:::/    /                       /:::/\:::\    \          /:::/\:::\    \         /:::/~~\:::\    \    
    /:::/  \:::\    \        /:::/__\:::\    \        /:::/____/                       /:::/__\:::\    \        /:::/__\:::\    \       /:::/    \:::\    \   
   /:::/    \:::\    \       \:::\   \:::\    \       |::|    |                       /::::\   \:::\    \      /::::\   \:::\    \     /:::/    / \:::\    \  
  /:::/    / \:::\    \    ___\:::\   \:::\    \      |::|    |     _____            /::::::\   \:::\    \    /::::::\   \:::\    \   /:::/____/   \:::\____\ 
 /:::/    /   \:::\    \  /\   \:::\   \:::\    \     |::|    |    /\    \          /:::/\:::\   \:::\ ___\  /:::/\:::\   \:::\____\ |:::|    |     |:::|    |
/:::/____/     \:::\____\/::\   \:::\   \:::\____\    |::|    |   /::\____\        /:::/__\:::\   \:::|    |/:::/  \:::\   \:::|    ||:::|____|     |:::|    |
\:::\    \      \::/    /\:::\   \:::\   \::/    /    |::|    |  /:::/    /        \:::\   \:::\  /:::|____|\::/   |::::\  /:::|____| \:::\    \   /:::/    / 
 \:::\    \      \/____/  \:::\   \:::\   \/____/     |::|    | /:::/    /          \:::\   \:::\/:::/    /  \/____|:::::\/:::/    /   \:::\    \ /:::/    /  
  \:::\    \               \:::\   \:::\    \         |::|____|/:::/    /            \:::\   \::::::/    /         |:::::::::/    /     \:::\    /:::/    /   
   \:::\    \               \:::\   \:::\____\        |:::::::::::/    /              \:::\   \::::/    /          |::|\::::/    /       \:::\__/:::/    /    
    \:::\    \               \:::\  /:::/    /        \::::::::::/____/                \:::\  /:::/    /           |::| \::/____/         \::::::::/    /     
     \:::\    \               \:::\/:::/    /          ~~~~~~~~~~                       \:::\/:::/    /            |::|  ~|                \::::::/    /      
      \:::\    \               \::::::/    /                                             \::::::/    /             |::|   |                 \::::/    /       
       \:::\____\               \::::/    /                                               \::::/    /              \::|   |                  \::/____/        
        \::/    /                \::/    /                                                 \::/____/                \:|   |                   ~~              
         \/____/                  \/____/                                                   ~~                       \|___|                                   
                 

============================================================================================================================================================

                 .----------------.  .----------------.  .----------------.   .----------------.  .----------------.  .----------------.
                | .--------------. || .--------------. || .--------------. | | .--------------. || .--------------. || .--------------. |
                | |   ______     | || |  ____  ____  | || |              | | | |  _______     | || |    ______    | || | _____  _____ | |
                | |  |_   _ \    | || | |_  _||_  _| | || |      _       | | | | |_   __ \    | || |  .' ___  |   | || ||_   _||_   _|| |
                | |    | |_) |   | || |   \ \  / /   | || |     (_)      | | | |   | |__) |   | || | / .'   \_|   | || |  | | /\ | |  | |
                | |    |  __'.   | || |    \ \/ /    | || |      _       | | | |   |  __ /    | || | | |    ____  | || |  | |/  \| |  | |
                | |   _| |__) |  | || |    _|  |_    | || |     (_)      | | | |  _| |  \ \_  | || | \ `.___]  _| | || |  |   /\   |  | |
                | |  |_______/   | || |   |______|   | || |              | | | | |____| |___| | || |  `._____.'   | || |  |__/  \__|  | |
                | |              | || |              | || |              | | | |              | || |              | || |              | |
                | '--------------' || '--------------' || '--------------' | | '--------------' || '--------------' || '--------------' |
                 '----------------'  '----------------'  '----------------'   '----------------'  '----------------'  '----------------'

            
"#
    );

    let menu_options = vec![
        "NEW",
        "OPEN (FROM CSV_DB)",
        "IMPORT (FROM LOCAL FILE SYSTEM)",
        "IMPORT (FROM URL)",
        "QUERY",
        "DELETE",
        "CONFIG",
    ];

    loop {
        let _builder = loop {
            print_list(&menu_options);
            let choice = get_user_input("Your move, bro: ");
            let _ = handle_quit_flag(&choice);
            let special_flag_without_builder_invoked = handle_special_flag_without_builder(&choice);

            let selected_option = determine_action_as_text(&menu_options, &choice);

            if !special_flag_without_builder_invoked {
                match selected_option {
                    Some(ref action) if action == "NEW" => {
                        //break

                        let home_dir =
                            env::var("HOME").expect("Unable to determine user home directory");
                        let desktop_path = Path::new(&home_dir).join("Desktop");
                        let csv_db_path = desktop_path.join("csv_db");

                        let file_name =
                            get_user_input("Enter file name to save (without extension): ");
                        let full_file_name = if file_name.ends_with(".csv") {
                            file_name
                        } else {
                            format!("{}.csv", file_name)
                        };
                        let file_path = csv_db_path.join(full_file_name);
                        let file_path_str = file_path.to_str();
                        let mut csv_builder = CsvBuilder::new();
                        let _ = csv_builder.save_as(file_path.to_str().unwrap());
                        chain_builder(csv_builder, file_path_str).await;
                    }
                    Some(ref action) if action == "OPEN (FROM CSV_DB)" => {
                        match open_csv_file(&csv_db_path_buf) {
                            Some((csv_builder, file_path)) => {
                                if let Some(path_str) = file_path.to_str() {
                                    chain_builder(csv_builder, Some(path_str)).await;
                                } else {
                                    println!("Error: Unable to convert file path to string.");
                                    continue;
                                }
                                continue; // Continue the outer loop since chain_builder has been called
                            }
                            None => continue,
                        }
                    }
                    Some(ref action) if action == "IMPORT (FROM LOCAL FILE SYSTEM)" => {
                        match import(&desktop_path_buf, &downloads_path_buf) {
                            Some(csv_builder) => chain_builder(csv_builder, None).await,
                            //Some(csv_builder) => break csv_builder,
                            None => continue,
                        }
                    }

                    Some(ref action) if action == "IMPORT (FROM URL)" => {
                        match import_from_url().await {
                            Some(csv_builder) => chain_builder(csv_builder, None).await,
                            //Some(csv_builder) => break csv_builder,
                            None => continue,
                        }
                    }

                    Some(ref action) if action == "QUERY" => match query(&csv_db_path_buf).await {
                        Ok(csv_builder) => break csv_builder,
                        Err(e) => {
                            if e.to_string() == "User chose to go back" {
                                continue;
                            }
                            continue;
                        }
                    },
                    Some(ref action) if action == "DELETE" => {
                        delete_csv_file(&csv_db_path_buf); // No return value expected
                        continue; // Continue the loop after deletion
                    }
                    Some(ref action) if action == "CONFIG" => {
                        let _ = edit_config(&csv_db_path_buf);
                        continue;
                    }

                    _ => {
                        print_insight(
                            "Dude, that action's a no-go. Give it another whirl, alright?",
                        );
                    }
                }
            }
        };
    }
}
