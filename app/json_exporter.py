import pandas as pd
import sys
from ui import UI

class JsonExporter:
    def __init__(self, ui: UI):
        self.__ui = ui
        self.__valid_directions = {'ASC', 'DESC'}

    def output_json(self, df: pd.DataFrame, columns: list = None, sort_column: str = None, sort_direction: str = 'ASC', limit: str = None):
        """Output the DataFrame in JSON format, optionally with specified columns, sorting, and limit, in blue."""
        df_to_output = df
        
        if columns:
            # Validate column names
            invalid_columns = [col for col in columns if col not in df.columns]
            if invalid_columns:
                self.__ui.print_colored(f"Error: Invalid column(s): {', '.join(invalid_columns)}", "red")
                sys.exit(1)
            df_to_output = df_to_output[columns]
        
        if sort_column:
            if sort_column not in df_to_output.columns:
                self.__ui.print_colored(f"Error: Sort column '{sort_column}' not found in selected columns.", "red")
                sys.exit(1)
            if sort_direction not in self.__valid_directions:
                self.__ui.print_colored(f"Error: Invalid sort direction '{sort_direction}'. Choose from ASC, DESC.", "red")
                sys.exit(1)
            ascending = sort_direction == 'ASC'
            df_to_output = df_to_output.sort_values(by=sort_column, ascending=ascending)
        
        if limit is not None:
            try:
                limit = int(limit)
                if limit <= 0:
                    raise ValueError
                df_to_output = df_to_output.iloc[:limit]
            except ValueError:
                self.__ui.print_colored(f"Error: LIMIT must be a positive integer, got '{limit}'", "red")
                sys.exit(1)
        
        # Convert to JSON with pretty-printing
        json_output = df_to_output.to_json(orient='records', indent=2)
        self.__ui.print_colored(json_output, "blue")
