// user_interaction.rs
use fuzzywuzzy::fuzz;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use vim_edit::vim_edit;

pub fn get_user_input(prompt: &str) -> String {
    let mut rl = match DefaultEditor::new() {
        Ok(editor) => editor,
        Err(err) => {
            println!("Failed to initialize editor: {:?}", err);
            return String::new();
        }
    };

    // ANSI escape codes for styling
    let bold_orange = "\x1b[1;38;5;208m";
    let reset = "\x1b[0m";

    // Custom prompt with styling
    let custom_prompt = format!("{}@BIGbro: {}{}{}", bold_orange, bold_orange, prompt, reset);

    loop {
        match rl.readline(&custom_prompt) {
            Ok(line) => {
                let _ = rl.add_history_entry(line.as_str());
                return line;
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                println!("Input interrupted or end of file reached.");
                return String::new();
            }
            Err(err) => {
                println!("Error reading line: {:?}", err);
                return String::new();
            }
        }
    }
}

pub fn get_edited_user_json_input(last_query: String) -> String {
    // Invoke vim_edit to edit the last query
    let edited_query = vim_edit(last_query);

    // Truncate everything after "SYNTAX\n======"
    let truncated_query = if let Some(index) = edited_query.find("SYNTAX\n======") {
        &edited_query[..index]
    } else {
        &edited_query[..]
    };

    // Prepare styled text for printing
    let bold_orange = "\x1b[0;38;5;208m";
    let reset = "\x1b[0m";
    let prompt = "Executing this JSON query:";

    // Check if the truncated query starts with "@c" after being trimmed
    if !truncated_query.trim().starts_with("@c") {
        print!(
            "  {}@LILbro: {}{}{}",
            bold_orange, bold_orange, prompt, reset
        );
        println!("\n{}", truncated_query.trim());
    }

    // Return the truncated and trimmed query
    truncated_query.trim().to_string()
}

pub fn get_edited_user_sql_input(last_query: String) -> String {
    // Invoke vim_edit to edit the last query

    let edited_query = vim_edit(last_query);

    // Truncate everything after "SYNTAX\n======"
    let truncated_query =
        if let Some(index) = edited_query.find("DIRECTIVES SYNTAX\n=================") {
            &edited_query[..index]
        } else {
            &edited_query[..]
        };

    //println!("\n\n{}", edited_query);
    if !truncated_query.trim().starts_with("@c") {
        println!("\n\n{}", truncated_query);
    }

    // Return the edited query
    truncated_query.trim().to_string()
}

pub fn get_edited_user_config_input(last_config: String) -> String {
    // Invoke vim_edit to edit the last query
    let edited_config = vim_edit(last_config);

    // Truncate everything after "SYNTAX\n======"
    let truncated_config = if let Some(index) = edited_config.find("SYNTAX\n======") {
        &edited_config[..index]
    } else {
        &edited_config[..]
    };

    // Prepare styled text for printing
    let bold_orange = "\x1b[0;38;5;208m";
    let reset = "\x1b[0m";
    let prompt = "Updating bro.config:";

    // Check if the truncated query starts with "@c" after being trimmed
    if !truncated_config.trim().starts_with("@c") {
        print!(
            "  {}@LILbro: {}{}{}",
            bold_orange, bold_orange, prompt, reset
        );
        println!("\n{}", truncated_config.trim());
    }

    // Return the truncated and trimmed query
    edited_config.trim().to_string()
}

pub fn get_user_input_level_2(prompt: &str) -> String {
    let mut rl = match DefaultEditor::new() {
        Ok(editor) => editor,
        Err(err) => {
            println!("Failed to initialize editor: {:?}", err);
            return String::new();
        }
    };

    // ANSI escape codes for styling
    let bold_orange = "\x1b[0;38;5;208m";
    let reset = "\x1b[0m";

    // Custom prompt with styling
    let custom_prompt = format!(
        "  {}@LILbro: {}{}{}",
        bold_orange, bold_orange, prompt, reset
    );

    loop {
        match rl.readline(&custom_prompt) {
            Ok(line) => {
                let _ = rl.add_history_entry(line.as_str());
                return line;
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                println!("Input interrupted or end of file reached.");
                return String::new();
            }
            Err(err) => {
                println!("Error reading line: {:?}", err);
                return String::new();
            }
        }
    }
}

pub fn print_list(options: &Vec<&str>) {
    // ANSI escape code for bold yellow font
    let bold_yellow = "\x1b[1;33m"; // Bold yellow
                                    // ANSI escape code to reset formatting
    let reset = "\x1b[0m";

    // Calculate the length of the longest option to ensure neat box sizing
    let max_length = options.iter().map(|o| o.len()).max().unwrap_or(0) + 14; // Adjusted for padding and border

    println!("{} +{}+{}", bold_yellow, "-".repeat(max_length), reset);
    println!("{} +{}+{}", bold_yellow, "-".repeat(max_length), reset);
    for (index, option) in options.iter().enumerate() {
        // Format each item with padding to align within the ASCII art box, ensuring the index is included correctly

        if index < 9 {
            let padded_option = format!(
                "  | {:<width$} |",
                format!("{}.  {}", index + 1, option),
                width = max_length - 4
            );
            println!("{}{}{}", bold_yellow, padded_option, reset);
        } else {
            let padded_option = format!(
                "  | {:<width$} |",
                format!("{}. {}", index + 1, option),
                width = max_length - 4
            );
            println!("{}{}{}", bold_yellow, padded_option, reset);
        }
        // println!("{}{}{}", bold_yellow, padded_option, reset);
    }
    println!("{} +{}+{}", bold_yellow, "-".repeat(max_length), reset);
    println!("{} +{}+{}", bold_yellow, "-".repeat(max_length), reset);
}

pub fn print_list_level_2(options: &Vec<&str>) {
    // ANSI escape code for bold yellow font
    let yellow = "\x1b[38;5;227m"; // Bold yellow
                                   // ANSI escape code to reset formatting
    let reset = "\x1b[0m";

    // Calculate the length of the longest option to ensure neat box sizing
    let max_length = options
        .iter()
        .flat_map(|o| o.lines())
        .map(|line| line.len())
        .max()
        .unwrap_or(0)
        + 14; // Adjusted for padding and border

    println!("{} +{}+{}", yellow, "-".repeat(max_length), reset);
    for (index, option) in options.iter().enumerate() {
        let lines: Vec<&str> = option.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            let prefix = if i == 0 {
                format!("{}. ", index + 1)
            } else {
                "  ".to_string()
            };
            let padded_option = format!(
                " | {}{:width$}   |",
                prefix,
                line,
                width = max_length - 4 - prefix.len()
            );
            println!("{}{}{}", yellow, padded_option, reset);
        }
    }
    println!("{} +{}+{}", yellow, "-".repeat(max_length), reset);
}

pub fn determine_action_as_text(menu_options: &[&str], choice: &str) -> Option<String> {
    let choice = choice.to_lowercase();

    // Check for direct numeric input
    if let Ok(index) = choice.parse::<usize>() {
        if index > 0 && index <= menu_options.len() {
            return Some(menu_options[index - 1].to_string());
        }
    }

    // Collect indices of "starts with" options
    let starts_with_indices: Vec<usize> = menu_options
        .iter()
        .enumerate()
        .filter_map(|(index, option)| {
            if option.to_lowercase().starts_with(&choice) {
                Some(index)
            } else {
                None
            }
        })
        .collect();

    let target_indices = if starts_with_indices.is_empty() {
        (0..menu_options.len()).collect::<Vec<usize>>()
    } else {
        starts_with_indices
    };

    let (best_match_index, _) = target_indices
        .iter()
        .map(|&index| {
            let option = &menu_options[index];
            (index, fuzz::ratio(&choice, &option.to_lowercase()))
        })
        .max_by_key(|&(_, score)| score)
        .unwrap_or((0, 0));

    if best_match_index < menu_options.len() {
        Some(menu_options[best_match_index].to_string())
    } else {
        None
    }
}

pub fn determine_action_type_feature_and_flag(choice: &str) -> (String, String, String) {
    // Ensure the choice is in lowercase
    let choice = choice.to_lowercase();

    // Find positions of 'f' and 'd'
    let f_pos = choice.find('f');
    let d_pos = choice.find('d');

    // Initialize the action_type, action_feature, and action_flag with empty strings
    let mut action_type = String::new();
    let mut action_feature = String::new();
    let mut action_flag = String::new();

    if let Some(f_index) = f_pos {
        // Action type is everything before 'f'
        action_type = choice[..f_index].to_string();
        // Check if there's a 'd' after 'f'
        if let Some(d_index) = d_pos {
            // Action feature is between 'f' and 'd'
            if d_index > f_index {
                action_feature = choice[f_index + 1..d_index].to_string();
            }
            // Action flag is 'd' itself
            action_flag = choice[d_index..].to_string();
        } else {
            // No 'd', so action feature is everything after 'f'
            action_feature = choice[f_index + 1..].to_string();
        }
    } else {
        // No 'f', so action type is the entire choice
        action_type = choice.to_string();
    }

    (action_type, action_feature, action_flag)
}

pub fn determine_action_as_number(menu_options: &[&str], choice: &str) -> Option<usize> {
    let choice = choice.to_lowercase();

    // Check for direct numeric input or numeric followed by "d"
    let parsed_choice = if choice.ends_with('d') {
        choice.trim_end_matches('d').parse::<usize>()
    } else {
        choice.parse::<usize>()
    };

    if let Ok(index) = parsed_choice {
        if index > 0 && index <= menu_options.len() {
            return Some(index);
        }
    }

    // Collect "starts with" matches
    let starts_with_indices: Vec<usize> = menu_options
        .iter()
        .enumerate()
        .filter_map(|(index, option)| {
            if option.to_lowercase().starts_with(&choice) {
                Some(index + 1)
            } else {
                None
            }
        })
        .collect();

    // If there's exactly one "starts with" match, return it
    if starts_with_indices.len() == 1 {
        return Some(starts_with_indices[0]);
    }

    // Apply fuzzy logic to either the filtered "starts with" options or all options
    let target_indices = if starts_with_indices.is_empty() {
        (1..=menu_options.len()).collect::<Vec<usize>>()
    } else {
        starts_with_indices
    };

    let (best_match_index, _) = target_indices
        .iter()
        .map(|&index| {
            let option = &menu_options[index - 1];
            (index, fuzz::ratio(&choice, &option.to_lowercase()))
        })
        .max_by_key(|&(_, score)| score)
        .unwrap_or((0, 0));

    if best_match_index > 0 && best_match_index <= menu_options.len() {
        Some(best_match_index)
    } else {
        None
    }
}

/// Prints a message in bold yellow font.
pub fn print_insight(message: &str) {
    // ANSI escape code for bold yellow font
    //let bold_yellow = "\x1b[1;93m"; // 1 for bold, 33 for yellow
    // ANSI escape code to reset formatting
    //let reset = "\x1b[0m";

    let bold_orange = "\x1b[1;38;5;208m";
    let reset = "\x1b[0m";

    println!("{}@BIGBro: {}{}", bold_orange, message, reset);
}

/// Prints a message in bold yellow font.
pub fn print_insight_level_2(message: &str) {
    // ANSI escape code for bold yellow font
    let bold_yellow = "\x1b[0;38;5;208m"; // 1 for bold, 33 for yellow
                                          // ANSI escape code to reset formatting
    let reset = "\x1b[0m";

    println!("  {}@LILBro: {}{}", bold_yellow, message, reset);
}
