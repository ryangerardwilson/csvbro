// settings.rs
use serde::{Deserialize, Serialize};
use serde_json;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;

use crate::user_experience::{handle_back_flag, handle_quit_flag};
use crate::user_interaction::{
    determine_action_as_text, get_edited_user_sql_input, get_user_input, get_user_input_level_2,
    print_insight, print_list,
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
                    //"BACK",
                ];
                print_list(&menu_options);
                let choice = get_user_input("Enter your choice: ").to_lowercase();

                if handle_back_flag(&choice) {
                    break;
                }
                let _ = handle_quit_flag(&choice);

                let selected_option = determine_action_as_text(&menu_options, &choice);

                match selected_option {
                    Some(ref action) if action == "add db preset" => {
                        add_db_preset()?;
                        continue;
                    }
                    Some(ref action) if action == "update db preset" => {
                        update_db_preset()?;
                        continue;
                    }
                    Some(ref action) if action == "delete db preset" => {
                        delete_db_preset()?;
                        continue;
                    }
                    Some(ref action) if action == "view db preset" => {
                        view_db_presets()?;
                        continue;
                    }
                    /*
                    Some(ref action) if action == "BACK" => {
                        break;
                    }
                    */
                    //"done" => break,
                    Some(_) => print_insight("Unrecognized action, please try again."),
                    None => print_insight("No action determined"),
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

                let selected_option = determine_action_as_text(&menu_options, &choice);

                match selected_option {
                    Some(ref action) if action == "add open ai preset" => {
                        add_open_ai_preset()?;
                        continue;
                    }
                    Some(ref action) if action == "update open ai preset" => {
                        update_open_ai_preset()?;
                        continue;
                    }
                    Some(ref action) if action == "delete open ai preset" => {
                        delete_open_ai_preset()?;
                        continue;
                    }
                    Some(ref action) if action == "view open ai preset" => {
                        view_open_ai_preset()?;
                        continue;
                    }
                    /*
                    Some(ref action) if action == "BACK" => {
                        break;
                    }
                    */
                    //"done" => break,
                    Some(_) => print_insight("Unrecognized action, please try again."),
                    None => print_insight("No action determined"),
                }
            },

            /*
            Some(ref action) if action == "BACK" => {
                break;
            }
            */
            //"done" => break,
            Some(_) => print_insight("Unrecognized action, please try again."),
            None => print_insight("No action determined"),
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
            DbConfig { db_presets: vec![] }
        } else {
            serde_json::from_str(&contents)?
        }
    } else {
        //println!("Config file does not exist, creating new Config instance.");
        DbConfig { db_presets: vec![] }
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
    manage_db_config_file(|config| {
        config.db_presets.push(new_preset);
        //println!("New preset added to config");
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

            // Add the formatted preset string to the vector
            formatted_presets.push(formatted_preset);
        }

        // Convert Vec<String> to Vec<&str> for print_list
        let formatted_presets_slices: Vec<&str> =
            formatted_presets.iter().map(AsRef::as_ref).collect();

        // Call print_list with a reference to the vector of string slices
        print_list(&formatted_presets_slices);

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

    //println!("Checking if path exists: {:?}", path);
    if !path.exists() {
        println!("Path does not exist, creating directory.");
        fs::create_dir_all(&path)?;
    }
    path.push("open_ai_config.json");
    //println!("Final path for config file: {:?}", path);

    let mut config = if path.exists() {
        let contents = fs::read_to_string(&path)?;
        if contents.is_empty() {
            //println!("Config file is empty, initializing new Config.");
            OpenAiConfig {
                open_ai_presets: vec![],
            }
        } else {
            serde_json::from_str(&contents)?
        }
    } else {
        //println!("Config file does not exist, creating new Config instance.");
        OpenAiConfig {
            open_ai_presets: vec![],
        }
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

fn add_open_ai_preset() -> Result<(), Box<dyn std::error::Error>> {
    let empty_preset = OpenAiPreset {
        api_key: String::new(),
    };

    let preset_json = serde_json::to_string_pretty(&empty_preset)?;
    let edited_json = get_edited_user_sql_input(preset_json);
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
        // Check if there is an existing preset to update
        if !config.open_ai_presets.is_empty() {
            // Serialize the existing preset to JSON for editing
            let preset_json = serde_json::to_string_pretty(&config.open_ai_presets[0])?;
            // Get edited JSON from the user
            let edited_json = get_edited_user_sql_input(preset_json);
            // Deserialize the edited JSON back into the OpenAiPreset struct
            config.open_ai_presets[0] = serde_json::from_str(&edited_json)?;
            Ok(())
        } else {
            // If there are no presets, inform the user and return an error
            print_insight("No OpenAI preset found.");
            Err("No OpenAI preset found.".into())
        }
    })
}

fn delete_open_ai_preset() -> Result<(), Box<dyn Error>> {
    manage_open_ai_config_file(|config| {
        // Check if there is an existing preset to update
        if !config.open_ai_presets.is_empty() {
            config.open_ai_presets[0].api_key = String::new();
            Ok(())
        } else {
            // If there are no presets, inform the user and return an error
            print_insight("No OpenAI preset found.");
            Err("No OpenAI preset found.".into())
        }
    })
}

pub fn view_open_ai_preset() -> Result<(), Box<dyn std::error::Error>> {
    match manage_open_ai_config_file(|config| {
        if !config.open_ai_presets.is_empty() {
            let message = format!(
                "Current OpenAI API Key: {}",
                config.open_ai_presets[0].api_key
            );
            print_insight(&message);
        } else {
            print_insight("No OpenAI preset found.");
        }
        Ok(())
    }) {
        Ok(_) => Ok(()),
        Err(_e) => {
            // Handle errors specifically if needed, for now, just print a generic message
            print_insight("No OpenAI preset found.");
            Ok(()) // You might want to return Err(e) if you want the caller to know the operation failed
        }
    }
}
