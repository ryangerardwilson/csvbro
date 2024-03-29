mod csv_inspector;
mod csv_joiner;
mod csv_manager;
mod csv_pivoter;
mod csv_searcher;
mod csv_tinkerer;
mod settings;
mod user_interaction;
mod utils;

use crate::csv_manager::{chain_builder, delete_csv_file, import, open_csv_file, query};
use crate::settings::open_settings;
use crate::user_interaction::{
    determine_action_as_text, get_user_input, print_insight, print_list,
};
use rgwml::csv_utils::CsvBuilder;
use std::env;
use std::path::Path;
use std::path::PathBuf;
use std::process::{self, Command};

#[tokio::main]
async fn main() {
    fn embed_and_set_up_in_directory_system() -> (String, String, String) {
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

        return (
            desktop_path.to_string_lossy().into_owned(),
            downloads_path.to_string_lossy().into_owned(),
            csv_db_path.to_string_lossy().into_owned(),
        );
    }

    if std::env::args().any(|arg| arg == "--version") {
        print_insight("csvbro 0.4.0");
        std::process::exit(0);
    }

    let (desktop_path, downloads_path, csv_db_path) = embed_and_set_up_in_directory_system();

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
        "NEW", "OPEN", "IMPORT", "QUERY", "DELETE", "SETTINGS", "EXIT",
    ];

    loop {
        let _builder = loop {
            print_list(&menu_options);
            let choice = get_user_input("Your move, bro: ");
            let selected_option = determine_action_as_text(&menu_options, &choice);

            match selected_option {
                Some(ref action) if action == "NEW" => {
                    //break

                    let home_dir =
                        env::var("HOME").expect("Unable to determine user home directory");
                    let desktop_path = Path::new(&home_dir).join("Desktop");
                    let csv_db_path = desktop_path.join("csv_db");

                    let file_name = get_user_input("Enter file name to save (without extension): ");
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
                Some(ref action) if action == "OPEN" => {
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
                Some(ref action) if action == "IMPORT" => {
                    match import(&desktop_path_buf, &downloads_path_buf) {
                        Some(csv_builder) => break csv_builder,
                        None => continue,
                    }
                }
                Some(ref action) if action == "QUERY" => match query().await {
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
                Some(ref action) if action == "SETTINGS" => {
                    let _ = open_settings(); // No return value expected
                    continue; // Continue the loop after settings are adjusted
                }
                Some(ref action) if action == "EXIT" => {
                    std::process::exit(0);
                    //return; // Exit the program
                }
                _ => {
                    print_insight("Dude, that action's a no-go. Give it another whirl, alright?");
                }
            }
        };
    }
}
