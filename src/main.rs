mod csv_appender;
mod csv_inspector;
mod csv_manager;
mod settings;
mod user_interaction;
mod utils;

use crate::csv_manager::{chain_builder, delete_csv_file, import, open_csv_file, query};
use crate::settings::open_settings;
use crate::user_interaction::{get_user_input, print_insight};
use fuzzywuzzy::fuzz;
use rgwml::csv_utils::CsvBuilder;
use std::env;
use std::path::Path;

#[tokio::main]
async fn main() {
    fn determine_action(input: &str, actions: &[&str]) -> String {
        let mut highest_score = 0;
        let mut best_match = String::new();

        for &action in actions {
            let mut score = fuzz::ratio(input, action);

            // Check if the first characters match and boost score if they do
            if input.chars().next() == action.chars().next() {
                score += 20;
            }

            if score > highest_score {
                highest_score = score;
                best_match = action.to_string();
            }
        }

        best_match
    }

    let home_dir = match env::var("HOME") {
        Ok(home) => home,
        Err(_) => match env::var("USERPROFILE") {
            Ok(userprofile) => userprofile,
            Err(_) => {
                eprintln!("Unable to determine user home directory.");
                std::process::exit(1);
            }
        },
    };

    let desktop_path = Path::new(&home_dir).join("Desktop");
    let downloads_path = Path::new(&home_dir).join("Downloads");
    let csv_db_path = desktop_path.join("csv_db");

    // Ensure csv_db directory exists
    std::fs::create_dir_all(&csv_db_path).expect("Failed to create directory");

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

    loop {
        let builder = loop {
            let input = get_user_input(
                "What's your move, homie?\n(new/open/import/query/delete/settings/exit): ",
            );

            match determine_action(
                &input,
                &[
                    "new", "open", "import", "query", "delete", "settings", "exit",
                ],
            )
            .as_str()
            {
                "new" => break CsvBuilder::new(),
                "open" => match open_csv_file(&csv_db_path) {
                    Some((csv_builder, file_path)) => {
                        // Convert file_path to a string slice before passing it
                        if let Some(path_str) = file_path.to_str() {
                            chain_builder(csv_builder, Some(path_str));
                        } else {
                            // Handle the error if the path cannot be converted to a string slice
                            println!("Error: Unable to convert file path to string.");
                            continue;
                        }
                        continue; // Continue the outer loop since chain_builder has been called
                    }
                    None => {
                        continue;
                    }
                },
                "import" => match import(&desktop_path, &downloads_path) {
                    Some(csv_builder) => break csv_builder,
                    None => {
                        continue;
                    }
                },
                "query" => match query().await {
                    Ok(csv_builder) => break csv_builder,
                    Err(e) => {
                        if e.to_string() == "User chose to go back" {
                            continue;
                        }
                        continue;
                    }
                },
                "delete" => {
                    delete_csv_file(&csv_db_path); // No return value expected
                    continue; // Continue the loop after deletion
                }
                "settings" => {
                    let _ = open_settings(); // No return value expected
                    continue; // Continue the loop after deletion
                }

                "exit" => return, // Exit the program
                _ => print_insight("Dude, that action's a no-go. Give it another whirl, alright?"),
            }
        };

        chain_builder(builder, None);
    }
}
