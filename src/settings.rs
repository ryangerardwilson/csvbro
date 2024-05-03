// settings.rs
use serde::{Deserialize, Serialize};
use serde_json;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

use crate::user_experience::{handle_back_flag, handle_cancel_flag, handle_quit_flag};
use crate::user_interaction::{
    determine_action_as_number, determine_action_as_text, get_edited_user_json_input,
    get_user_input, get_user_input_level_2, print_insight, print_insight_level_2, print_list,
    print_list_level_2,
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct OpenAiPreset {
    pub api_key: String,
}

#[derive(Serialize, Deserialize)]
pub struct DbConfig {
    pub db_presets: Vec<DbPreset>,
}

#[derive(Serialize, Deserialize)]
pub struct OpenAiConfig {
    pub open_ai_presets: Vec<OpenAiPreset>,
}

pub fn open_settings() -> Result<(), Box<dyn std::error::Error>> {
    // Additional settings options here
    loop {
        print_insight("Decision time! What are you vibing with?");
        let menu_options = vec!["db presets", "open ai presets"];
        print_list(&menu_options);
        let choice = get_user_input("Enter your choice: ").to_lowercase();

        if handle_back_flag(&choice) {
            break;
            //break Ok(CsvBuilder::new());
        }
        let _ = handle_quit_flag(&choice);

        let selected_option = determine_action_as_text(&menu_options, &choice);

        match selected_option {
            Some(ref action) if action == "db presets" => loop {
                print_insight("Configure Db Presets");
                let menu_options = vec![
                    "add db preset",
                    "update db preset",
                    "delete db preset",
                    "view db presets",
                ];
                print_list_level_2(&menu_options);
                let choice = get_user_input("Enter your choice: ").to_lowercase();

                if handle_back_flag(&choice) {
                    break;
                }
                let _ = handle_quit_flag(&choice);

                let selected_option = determine_action_as_number(&menu_options, &choice);

                match selected_option {
                    Some(1) => {
                        add_db_preset()?;
                        continue;
                    }
                    Some(2) => {
                        update_db_preset()?;
                        continue;
                    }
                    Some(3) => {
                        delete_db_preset()?;
                        continue;
                    }
                    Some(4) => {
                        view_db_presets()?;
                        continue;
                    }

                    _ => {
                        println!("Invalid option. Please enter a number from 1 to 4.");
                        continue; // Ask for the choice again
                    }
                }

            },
            Some(ref action) if action == "open ai presets" => loop {
                print_insight("Configure OpenAI Presets");
                let menu_options = vec![
                    "add open ai preset",
                    "update open ai preset",
                    "delete open ai preset",
                    "view open ai preset",
                    //"BACK",
                ];
                print_list(&menu_options);
                let choice = get_user_input("Enter your choice: ").to_lowercase();

                if handle_back_flag(&choice) {
                    break;
                    //break Ok(CsvBuilder::new());
                }
                let _ = handle_quit_flag(&choice);

                let selected_option = determine_action_as_number(&menu_options, &choice);

                match selected_option {
                    Some(1) => {
                        add_open_ai_preset()?;
                        continue;
                    }
                    Some(2) => {
                        update_open_ai_preset()?;
                        continue;
                    }
                    Some(3) => {
                        delete_open_ai_preset()?;
                        continue;
                    }
                    Some(4) => {
                        view_open_ai_preset()?;
                        continue;
                    }
                    _ => {
                        println!("Invalid option. Please enter a number from 1 to 4.");
                        continue; // Ask for the choice again
                    }
                }

            },

            _ => {
                continue; // Ask for the choice again
            } 

        }
    }

    Ok(())
}

pub fn manage_db_config_file<F: FnOnce(&mut DbConfig) -> Result<(), Box<dyn Error>>>(
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

    if !path.exists() {
        println!("Path does not exist, creating directory.");
        fs::create_dir_all(&path)?;
    }
    path.push("db_config.json");

    let mut config = if path.exists() {
        let contents = fs::read_to_string(&path)?;
        if contents.is_empty() {
            DbConfig { db_presets: vec![] }
        } else {
            serde_json::from_str(&contents)?
        }
    } else {
        DbConfig { db_presets: vec![] }
    };

    op(&mut config)?;

    let serialized = serde_json::to_string(&config)?;

    fs::write(path, serialized)?;

    Ok(())
}

fn add_db_preset() -> Result<(), Box<dyn std::error::Error>> {
    let empty_preset = DbPreset {
        name: String::new(),
        db_type: String::new(),
        host: String::new(),
        username: String::new(),
        password: String::new(),
        database: String::new(),
    };

    let preset_json = serde_json::to_string_pretty(&empty_preset)?;

    let edited_json = get_edited_user_json_input(preset_json);

    if handle_cancel_flag(&edited_json) {
        return Ok(());
    }

    let new_preset: DbPreset = serde_json::from_str(&edited_json)?;

    manage_db_config_file(|config| {
        config.db_presets.push(new_preset);
        Ok(())
    })
}

fn update_db_preset() -> Result<(), Box<dyn Error>> {
    view_db_presets()?;
    let input = get_user_input("Enter the name or the number of the preset to update: ");

    manage_db_config_file(|config| {
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
            let edited_json = get_edited_user_json_input(preset_json);

            if handle_cancel_flag(&edited_json) {
                return Ok(());
            }

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

    if handle_cancel_flag(&input) {
        return Ok(());
    }

    manage_db_config_file(|config| {
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
    manage_db_config_file(|config| {
        println!();
        // Initialize a vector to hold the formatted preset strings
        let mut formatted_presets = Vec::new();

        for (_index, preset) in config.db_presets.iter().enumerate() {
            let formatted_preset = format!(
        "{}\n\n{{\n  db_type: \"{}\",\n  host: \"{}\",\n  username: \"{}\",\n  password: \"{}\",\n  database: \"{}\"\n}}\n\n", 
        preset.name,
        preset.db_type,
        preset.host,
        preset.username,
        preset.password,
        preset.database
    );

            // Add the formatted preset string to the vector
            formatted_presets.push(formatted_preset);
        }

        // Convert Vec<String> to Vec<&str> for print_list
        let formatted_presets_slices: Vec<&str> =
            formatted_presets.iter().map(AsRef::as_ref).collect();

        // Call print_list with a reference to the vector of string slices
        print_list_level_2(&formatted_presets_slices);

        println!();
        Ok(())
    })
}

pub fn manage_open_ai_config_file<F: FnOnce(&mut OpenAiConfig) -> Result<(), Box<dyn Error>>>(
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

    if !path.exists() {
        println!("Path does not exist, creating directory.");
        fs::create_dir_all(&path)?;
    }
    path.push("open_ai_config.json");

    let mut config = if path.exists() {
        let contents = fs::read_to_string(&path)?;
        if contents.is_empty() {
            OpenAiConfig {
                open_ai_presets: vec![],
            }
        } else {
            serde_json::from_str(&contents)?
        }
    } else {
        OpenAiConfig {
            open_ai_presets: vec![],
        }
    };

    op(&mut config)?;

    let serialized = serde_json::to_string(&config)?;

    fs::write(path, serialized)?;

    Ok(())
}

fn add_open_ai_preset() -> Result<(), Box<dyn std::error::Error>> {
    let empty_preset = OpenAiPreset {
        api_key: String::new(),
    };

    let preset_json = serde_json::to_string_pretty(&empty_preset)?;
    let edited_json = get_edited_user_json_input(preset_json);

    if handle_cancel_flag(&edited_json) {
        return Ok(());
    }

    let new_preset: OpenAiPreset = serde_json::from_str(&edited_json)?;

    manage_open_ai_config_file(|config| {
        if config.open_ai_presets.is_empty() {
            config.open_ai_presets.push(new_preset);
        } else {
            config.open_ai_presets[0] = new_preset;
        }
        Ok(())
    })
}

fn update_open_ai_preset() -> Result<(), Box<dyn Error>> {
    manage_open_ai_config_file(|config| {
        if !config.open_ai_presets.is_empty() {
            let preset_json = serde_json::to_string_pretty(&config.open_ai_presets[0])?;
            let edited_json = get_edited_user_json_input(preset_json);

            if handle_cancel_flag(&edited_json) {
                return Ok(());
            }

            config.open_ai_presets[0] = serde_json::from_str(&edited_json)?;
            Ok(())
        } else {
            print_insight("No OpenAI preset found.");
            Err("No OpenAI preset found.".into())
        }
    })
}

fn delete_open_ai_preset() -> Result<(), Box<dyn Error>> {
    manage_open_ai_config_file(|config| {
        if !config.open_ai_presets.is_empty() {
            config.open_ai_presets[0].api_key = String::new();
            Ok(())
        } else {
            print_insight("No OpenAI preset found.");
            Err("No OpenAI preset found.".into())
        }
    })
}

pub fn view_open_ai_preset() -> Result<(), Box<dyn std::error::Error>> {
    match manage_open_ai_config_file(|config| {
        if !config.open_ai_presets.is_empty() {
            let message = format!(
                "Current OpenAI API Key: {}\n",
                config.open_ai_presets[0].api_key
            );
            println!();
            print_insight_level_2(&message);
        } else {
            print_insight_level_2("No OpenAI preset found.");
        }
        Ok(())
    }) {
        Ok(_) => Ok(()),
        Err(_e) => {
            print_insight("No OpenAI preset found.");
            Ok(()) 
        }
    }
}
