// user_interaction.rs
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use vim_edit::{vim_create, vim_edit};
use fuzzywuzzy::fuzz;


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

/*
/// Prints a message in bold yellow font.
pub fn print_list(message: &str) {
    // ANSI escape code for bold yellow font
    let bold_yellow = "\x1b[0;93m"; // 1 for bold, 33 for yellow
                                    // ANSI escape code to reset formatting
    let reset = "\x1b[0m";

    println!("  {}{}{}", bold_yellow, message, reset);
}


pub fn print_list(options: &Vec<&str>) {
    // ANSI escape code for bold yellow font
    let bold_yellow = "\x1b[1;33m"; // Corrected the ANSI code for bold yellow
    // ANSI escape code to reset formatting
    let reset = "\x1b[0m";

    for (index, option) in options.iter().enumerate() {
        println!("  {}{}: {}{}", bold_yellow, index + 1, option, reset);
    }
}
*/

pub fn print_list(options: &Vec<&str>) {
    // ANSI escape code for bold yellow font
    let bold_yellow = "\x1b[1;33m"; // Bold yellow
    // ANSI escape code to reset formatting
    let reset = "\x1b[0m";

    // Calculate the length of the longest index to ensure neat indentation
    let max_digits = options.len().to_string().len();

    for (index, option) in options.iter().enumerate() {
        let padded_index = format!("{:width$}:", index + 1, width = max_digits);
        println!("  {}{} {}{}", bold_yellow, padded_index, option, reset);
    }
}

pub fn determine_action_as_text(menu_options: &[&str], choice: &str) -> Option<String> {
    let choice = choice.to_lowercase();
    let mut selected_option: Option<String> = None;

    // Check for direct numeric input
    if let Ok(index) = choice.parse::<usize>() {
        if index > 0 && index <= menu_options.len() {
            selected_option = Some(menu_options[index - 1].to_string());
        }
    }

    // If no direct numeric input, use fuzzy matching
    if selected_option.is_none() {
        let (best_match_index, _) = menu_options
            .iter()
            .enumerate()
            .map(|(index, option)| (index + 1, fuzz::ratio(&choice, &option.to_lowercase())))
            .max_by_key(|&(_, score)| score)
            .unwrap_or((0, 0));

        if best_match_index > 0 && best_match_index <= menu_options.len() {
            selected_option = Some(menu_options[best_match_index - 1].to_string());
        }
    }

    selected_option
}


pub fn determine_action_as_text_or_number(menu_options: &[&str], choice: &str) -> Option<usize> {
    let choice = choice.to_lowercase();
    let mut selected_option = None;

    // Check for direct numeric input
    if let Ok(index) = choice.parse::<usize>() {
        if index > 0 && index <= menu_options.len() {
            selected_option = Some(index);
        }
    }

    // If no direct numeric input, use fuzzy matching
    if selected_option.is_none() {
        let (best_match_index, _) = menu_options
            .iter()
            .enumerate()
            .map(|(index, option)| (index + 1, fuzz::ratio(&choice, &option.to_lowercase())))
            .max_by_key(|&(_, score)| score)
            .unwrap_or((0, 0));

        if best_match_index > 0 && best_match_index <= menu_options.len() {
            selected_option = Some(best_match_index);
        }
    }

    selected_option
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
