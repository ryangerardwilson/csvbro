// config.rs
use crate::user_interaction::{get_edited_user_config_input, print_insight_level_2};
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;

//#[allow(dead_code)]
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Config {
    pub db_presets: Vec<DbPreset>,
    pub open_ai_key: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct DbPreset {
    pub name: String,
    pub db_type: String,
    pub host: String,
    pub username: String,
    pub password: String,
    pub database: String,
}

pub fn edit_config(csv_db_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let config_path = csv_db_path.join("bro.config");

    // Step 1: Create a bro.config if it does not already exist, containing the below text

    let bro_config_default_text = r#"{
  "db_presets" : [
    {
      "name": "",
      "db_type": "",
      "host": "",
      "username": "",
      "password": "",
      "database": ""
    }
  ],
  "open_ai_key": ""
}

SYNTAX
======
{
  "db_presets" : [
    {
      "name": "",
      "db_type": "", // "mysql", "mssql", and "clickhouse" are supported
      "host": "",
      "username": "",
      "password": "",
      "database": ""
    }
  ],
  "open_ai_key": ""
}
"#;

    if !config_path.exists() {
        let mut file = File::create(&config_path)?;
        file.write_all(bro_config_default_text.as_bytes())?;
    }

    // Step 2: Get bro.config content into a variable
    let mut current_config_text = String::new();
    File::open(&config_path)?.read_to_string(&mut current_config_text)?;

    // Step 3: Open bro.config in vim for editing
    let mut edited_config_text = current_config_text.clone();
    edited_config_text = get_edited_user_config_input(edited_config_text.clone());

    if let Some(json_part) = edited_config_text.split("SYNTAX").next() {
        match serde_json::from_str::<Value>(json_part) {
            Ok(_) => {
                print_insight_level_2("Config's all good, bro!");
            }
            Err(e) => {
                println!();
                print_insight_level_2(&format!("Whoops, hit a snag with that JSON: {}. Mind tweaking the config and trying again?", e));
                return Err(e.into());
            }
        }
    }

    // Step 5: Remove the SYNTAX, and everything that follows it and replace it with the fresh  SYNTAX content below

    let fresh_syntax = r#"SYNTAX
======

{
  "db_presets" : [
    {
      "name": "",
      "db_type": "", // "mysql", "mssql", and "clickhouse" are supported
      "host": "",
      "username": "",
      "password": "",
      "database": ""
    }
  ],
  "open_ai_key": ""
}
    "#;

    let json_part = edited_config_text
        .split("SYNTAX")
        .next()
        .unwrap_or_default();
    let new_config_content = format!("{}{}", json_part, fresh_syntax);

    //dbg!(&new_config_content);

    // Step 6: Save the file to the indicated location as bro.config overwriting any previous file
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&config_path)?;
    file.write_all(new_config_content.as_bytes())?;

    Ok(())
}
