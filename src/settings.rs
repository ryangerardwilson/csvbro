// settings.rs
use fuzzywuzzy::fuzz;
use serde::{Deserialize, Serialize};
use serde_json;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

use crate::user_interaction::{
    get_edited_user_sql_input, get_user_input, get_user_input_level_2, print_insight, print_list,
};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DbPreset {
    pub name: String,
    pub db_type: String,
    pub host: String,
    pub username: String,
    pub password: String,
    pub database: String,
}

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub db_presets: Vec<DbPreset>,
}

pub fn open_settings() -> Result<(), Box<dyn std::error::Error>> {
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

    // Additional settings options here

    let choice = get_user_input("Decision time! What are you vibing with?\ndb_presets: ");
    let actions = ["db_presets"];
    let matched_action = determine_action(&choice, &actions);

    match matched_action.as_str() {
        "db_presets" => loop {
            // Sub-menu for DB preset management
            let db_choice =
                get_user_input("db_presets => choose action\nadd/update/delete/view/back: ");
            let db_actions = ["add", "update", "delete", "view", "back"];
            let matched_db_action = determine_action(&db_choice, &db_actions);

            match matched_db_action.as_str() {
                "add" => {
                    add_db_preset()?;
                    continue;
                }
                "update" => {
                    update_db_preset()?;
                    continue;
                }
                "delete" => {
                    delete_db_preset()?;
                    continue;
                }
                "view" => {
                    view_db_presets()?;
                    continue;
                }
                "back" => break,
                _ => print_insight("Invalid option"),
            }
        },
        // Handle other settings options here
        _ => print_insight("Invalid option"),
    }
    Ok(())
}

pub fn manage_config_file<F: FnOnce(&mut Config) -> Result<(), Box<dyn Error>>>(
    op: F,
) -> Result<(), Box<dyn Error>> {
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
    let mut path = desktop_path.join("csv_db");

    //println!("Checking if path exists: {:?}", path);
    if !path.exists() {
        println!("Path does not exist, creating directory.");
        fs::create_dir_all(&path)?;
    }
    path.push("db_config.json");
    //println!("Final path for config file: {:?}", path);

    let mut config = if path.exists() {
        let contents = fs::read_to_string(&path)?;
        if contents.is_empty() {
            //println!("Config file is empty, initializing new Config.");
            Config { db_presets: vec![] }
        } else {
            serde_json::from_str(&contents)?
        }
    } else {
        //println!("Config file does not exist, creating new Config instance.");
        Config { db_presets: vec![] }
    };

    //println!("Performing operation on config.");
    op(&mut config)?;

    //println!("Serializing config.");
    let serialized = serde_json::to_string(&config)?;
    //println!("Serialized config: {}", serialized);

    //println!("Writing config to file.");
    fs::write(path, serialized)?;
    //println!("Config written to file successfully.");

    Ok(())
}

fn add_db_preset() -> Result<(), Box<dyn std::error::Error>> {
    // Create an empty JSON template
    let empty_preset = DbPreset {
        name: String::new(),
        db_type: String::new(),
        host: String::new(),
        username: String::new(),
        password: String::new(),
        database: String::new(),
    };
    // println!("Empty preset created: {:?}", empty_preset);

    // Convert the empty preset to a JSON string
    let preset_json = serde_json::to_string_pretty(&empty_preset)?;
    //println!("Empty preset as JSON: {}", preset_json);

    // Let the user edit the JSON
    let edited_json = get_edited_user_sql_input(preset_json);
    //println!("Edited JSON received: {}", edited_json);

    // Parse the edited JSON back into a DbPreset
    let new_preset: DbPreset = serde_json::from_str(&edited_json)?;
    //println!("New preset parsed from JSON: {:?}", new_preset);

    // Add the new preset to the configuration file
    manage_config_file(|config| {
        config.db_presets.push(new_preset);
        //println!("New preset added to config");
        Ok(())
    })
}

fn update_db_preset() -> Result<(), Box<dyn Error>> {
    view_db_presets()?;
    let input = get_user_input("Enter the name or the number of the preset to update: ");

    manage_config_file(|config| {
        let maybe_preset = if let Ok(index) = input.parse::<usize>() {
            // User entered a number, adjust for 0-based index
            config.db_presets.get_mut(index - 1)
        } else {
            // User entered a name
            config.db_presets.iter_mut().find(|p| p.name == input)
        };

        if let Some(preset) = maybe_preset {
            // Convert the preset into a JSON string
            let preset_json = serde_json::to_string_pretty(&preset)?;

            // Let the user edit the JSON
            let edited_json = get_edited_user_sql_input(preset_json);

            // Parse the edited JSON back into the preset
            *preset = serde_json::from_str(&edited_json)?;
        } else {
            print_insight("Preset not found.");
        }
        Ok(())
    })
}

fn delete_db_preset() -> Result<(), Box<dyn std::error::Error>> {
    view_db_presets()?;
    let input = get_user_input_level_2("Enter the name or the number of the preset to delete: ");

    manage_config_file(|config| {
        if let Ok(index) = input.parse::<usize>() {
            // User entered a number, adjust for 0-based index
            if index == 0 || index > config.db_presets.len() {
                print_insight("Invalid index.");
            } else {
                config.db_presets.remove(index - 1);
            }
        } else {
            // User entered a name
            config.db_presets.retain(|preset| preset.name != input);
        }
        Ok(())
    })
}

pub fn view_db_presets() -> Result<(), Box<dyn std::error::Error>> {
    manage_config_file(|config| {
        println!();
        for (index, preset) in config.db_presets.iter().enumerate() {
            let formatted_preset = format!(
                "{}. {} {{db_type: \"{}\", host: \"{}\", username: \"{}\", password: \"{}\", database: \"{}\" }}",
                index + 1,
                preset.name,
                preset.db_type,
                preset.host,
                preset.username,
                preset.password,
                preset.database
            );
            print_list(&formatted_preset);
        }
        println!();
        Ok(())
    })
}
