import pandas as pd
import sys
from ui import UI

class DataFrameViewer:
    def __init__(self, ui: UI):
        self.__ui = ui
        self.__valid_directions = {'ASC', 'DESC'}

    def show_dataframe(self, df: pd.DataFrame, columns: list = None, sort_column: str = None, sort_direction: str = 'ASC', limit: str = None):
        """Display the DataFrame content, optionally with specified columns, sorting, and limit, in blue."""
        print()
        df_to_show = df
        if columns:
            # Validate column names
            invalid_columns = [col for col in columns if col not in df.columns]
            if invalid_columns:
                self.__ui.print_colored(f"Error: Invalid column(s): {', '.join(invalid_columns)}", "red")
                sys.exit(1)
            df_to_show = df[columns]
        
        if sort_column:
            if sort_column not in df_to_show.columns:
                self.__ui.print_colored(f"Error: Sort column '{sort_column}' not found in selected columns.", "red")
                sys.exit(1)
            if sort_direction not in self.__valid_directions:
                self.__ui.print_colored(f"Error: Invalid sort direction '{sort_direction}'. Choose from ASC, DESC.", "red")
                sys.exit(1)
            ascending = sort_direction == 'ASC'
            df_to_show = df_to_show.sort_values(by=sort_column, ascending=ascending)
        
        if limit is not None:
            try:
                limit = int(limit)
                if limit <= 0:
                    raise ValueError
                df_to_show = df_to_show.iloc[:limit]
                # Temporarily set max_rows to ensure all limited rows are displayed
                original_max_rows = pd.options.display.max_rows
                pd.options.display.max_rows = limit
            except ValueError:
                self.__ui.print_colored(f"Error: LIMIT must be a positive integer, got '{limit}'", "red")
                sys.exit(1)
        
        # Print DataFrame and columns in blue
        self.__ui.print_colored(str(df_to_show), "blue")
        self.__ui.print_colored(str(list(df.columns)), "blue")
        
        # Reset display settings if limit was used
        if limit is not None:
            pd.options.display.max_rows = original_max_rows
