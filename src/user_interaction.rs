// user_interaction.rs
use fuzzywuzzy::fuzz;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use vim_edit::{vim_create, vim_edit};

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

pub fn get_user_sql_input() -> String {
    // ANSI escape code for bold orange font
    let bold_orange = "\x1b[0;38;5;208m"; // 1 for bold, 38;5;208 for orange font
                                          // ANSI escape code to reset formatting
    let reset = "\x1b[0m";

    let prompt = "Executing this query:";

    print!(
        "  {}@LILbro: {}{}{}",
        bold_orange, bold_orange, prompt, reset
    );

    let input: String = vim_create();
    println!("\n\n{}", input);

    input.trim().to_string()
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

    let bold_orange = "\x1b[0;38;5;208m";
    let reset = "\x1b[0m";

    let prompt = "Executing this JSON query:";

    print!(
        "  {}@LILbro: {}{}{}",
        bold_orange, bold_orange, prompt, reset
    );
    let result = truncated_query.trim().to_string();
    println!("\n{}", result);
    result
}

pub fn get_edited_user_sql_input(last_query: String) -> String {
    // Invoke vim_edit to edit the last query

    let edited_query = vim_edit(last_query);
    println!("\n\n{}", edited_query);

    // Return the edited query
    edited_query
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
        let padded_option = format!(
            "  | {:<width$} |",
            format!("{}. {}", index + 1, option),
            width = max_length - 4
        );
        println!("{}{}{}", bold_yellow, padded_option, reset);
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
    let max_length = options.iter().map(|o| o.len()).max().unwrap_or(0) + 14; // Adjusted for padding and border

    //println!("{} +{}+{}", yellow, "-".repeat(max_length), reset);
    println!(" {} +{}+{}", yellow, "-".repeat(max_length), reset);
    for (index, option) in options.iter().enumerate() {
        // Format each item with padding to align within the ASCII art box, ensuring the index is included correctly
        let padded_option = format!(
            "   | {:<width$} |",
            format!("{}. {}", index + 1, option),
            width = max_length - 4
        );
        println!("{}{}{}", yellow, padded_option, reset);
    }
    //println!("{} +{}+{}", yellow, "-".repeat(max_length), reset);
    println!(" {} +{}+{}", yellow, "-".repeat(max_length), reset);
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

/*
pub fn determine_action_as_number(menu_options: &[&str], choice: &str) -> Option<usize> {
    let choice = choice.to_lowercase();

    // Check for direct numeric input
    if let Ok(index) = choice.parse::<usize>() {
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
*/

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
