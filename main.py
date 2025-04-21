#!/usr/bin/env python3
import sys
import pandas as pd
import numpy as np
from ui import UI
import threading

class App:
    def __init__(self):
        self.__ui = UI()
        self.__df = None
        self.__filename = None
        self.__valid_aggfuncs = {'SUM', 'COUNT', 'COUNT_UNIQUE', 'MEAN', 'MEDIAN'}
        self.__valid_directions = {'ASC', 'DESC'}

    def __load_csv(self, filename):
        """Load a CSV file into a pandas DataFrame."""
        try:
            stop_animation = threading.Event()
            animation_thread = threading.Thread(target=self.__ui.animate_loading, args=(stop_animation, f"Loading {filename}"))
            animation_thread.start()
            
            self.__df = pd.read_csv(filename)
            
            stop_animation.set()
            animation_thread.join()
        except FileNotFoundError:
            self.__ui.print_colored(f"Error: File '{filename}' not found.", "red")
            sys.exit(1)
        except Exception as e:
            self.__ui.print_colored(f"Error loading CSV: {str(e)}", "red")
            sys.exit(1)

    def __show_dataframe(self, columns=None, sort_column=None, sort_direction='ASC', limit=None):
        """Display the DataFrame content, optionally with specified columns, sorting, and limit, in blue."""
        print()
        df_to_show = self.__df
        if columns:
            # Validate column names
            invalid_columns = [col for col in columns if col not in self.__df.columns]
            if invalid_columns:
                self.__ui.print_colored(f"Error: Invalid column(s): {', '.join(invalid_columns)}", "red")
                sys.exit(1)
            df_to_show = self.__df[columns]
        
        if sort_column:
            if sort_column not in df_to_show.columns:
                self.__ui.print_colored(f"Error: Sort column '{sort_column}' not found in selected columns.", "red")
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
        self.__ui.print_colored(str(list(self.__df.columns)), "blue")
        
        # Reset display settings if limit was used
        if limit is not None:
            pd.options.display.max_rows = original_max_rows

    def __output_json(self, columns=None, sort_column=None, sort_direction='ASC', limit=None):
        """Output the DataFrame in JSON format, optionally with specified columns, sorting, and limit, in blue."""
        df_to_output = self.__df
        
        if columns:
            # Validate column names
            invalid_columns = [col for col in columns if col not in self.__df.columns]
            if invalid_columns:
                self.__ui.print_colored(f"Error: Invalid column(s): {', '.join(invalid_columns)}", "red")
                sys.exit(1)
            df_to_output = df_to_output[columns]
        
        if sort_column:
            if sort_column not in df_to_output.columns:
                self.__ui.print_colored(f"Error: Sort column '{sort_column}' not found in selected columns.", "red")
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



    def __create_pivot_table(self, row, value, aggfunc, sort_column=None, sort_direction='ASC', limit=None):
        """Create a pivot table from the DataFrame with specified aggregation, sorting, and limit."""
        try:
            # Validate column names
            for col_name in [row, value]:
                if col_name not in self.__df.columns:
                    self.__ui.print_colored(f"Error: Column '{col_name}' not found in DataFrame.", "red")
                    sys.exit(1)
            
            # Validate aggregation function
            if aggfunc not in self.__valid_aggfuncs:
                self.__ui.print_colored(f"Error: Invalid aggregation function '{aggfunc}'. Choose from {', '.join(self.__valid_aggfuncs)}.", "red")
                sys.exit(1)
            
            # Check if value column is numeric for SUM, MEAN, MEDIAN
            if aggfunc in {'SUM', 'MEAN', 'MEDIAN'} and not pd.api.types.is_numeric_dtype(self.__df[value]):
                self.__ui.print_colored(f"Error: Value column '{value}' must be numeric for {aggfunc} aggregation.", "red")
                sys.exit(1)
            
            stop_animation = threading.Event()
            animation_thread = threading.Thread(target=self.__ui.animate_loading, args=(stop_animation, "Creating pivot table"))
            animation_thread.start()
            
            # Define aggregation function
            if aggfunc == 'SUM':
                agg_func = 'sum'
            elif aggfunc == 'COUNT':
                agg_func = 'count'
            elif aggfunc == 'COUNT_UNIQUE':
                agg_func = pd.Series.nunique
            elif aggfunc == 'MEAN':
                agg_func = 'mean'
            elif aggfunc == 'MEDIAN':
                agg_func = 'median'
            
            pivot = pd.pivot_table(self.__df, index=row, values=value, aggfunc=agg_func, fill_value=0)
            
            # Handle sorting
            if sort_column:
                if sort_column != value:
                    self.__ui.print_colored(f"Error: Sort column '{sort_column}' must be the value column '{value}' for pivot table.", "red")
                    sys.exit(1)
                ascending = sort_direction == 'ASC'
                pivot = pivot.sort_values(by=value, ascending=ascending)
            
            if limit is not None:
                try:
                    limit = int(limit)
                    if limit <= 0:
                        raise ValueError
                    pivot = pivot.iloc[:limit]
                except ValueError:
                    self.__ui.print_colored(f"Error: LIMIT must be a positive integer, got '{limit}'", "red")
                    sys.exit(1)
            
            # Stop the animation before printing
            stop_animation.set()
            animation_thread.join()
            
            # Ensure all limited rows are displayed
            original_max_rows = pd.options.display.max_rows
            pd.options.display.max_rows = None
            
            self.__ui.print_colored(f"\nPivot Table ({aggfunc}):", "green")
            self.__ui.print_colored("=" * 50, "green")
            self.__ui.print_colored(str(pivot), "blue")
            self.__ui.print_colored("=" * 50, "green")
            
            # Reset pandas display option
            pd.options.display.max_rows = original_max_rows
        except Exception as e:
            self.__ui.print_colored(f"Error creating pivot table: {str(e)}", "red")
            sys.exit(1)

    def __print_usage(self):
        """Print usage instructions."""
        self.__ui.print_colored("Usage: csvbro <filename> [<command> [args]]", "green")
        self.__ui.print_colored("If no command is provided, the CSV content is displayed as a normal DataFrame (with pandas' default truncation) unless ORDER_BY or LIMIT is specified.", "green")
        self.__ui.print_colored("Commands:", "green")
        self.__ui.print_colored("  SHOW <column1> [<column2> ...] [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>]  Display specified columns of the CSV", "blue")
        self.__ui.print_colored("  PIVOT <row> <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>]  Create a pivot table (value must be numeric for SUM, MEAN, MEDIAN)", "blue")
        self.__ui.print_colored("  JSON [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>]  Output the DataFrame in JSON format", "blue")
        self.__ui.print_colored("  ORDER_BY <sort_column> <ASC|DESC> [LIMIT <n>]  Sort the entire DataFrame", "blue")
        self.__ui.print_colored("  LIMIT <n>  Limit the entire DataFrame to <n> rows", "blue")
        self.__ui.print_colored("  Aggregation functions: SUM, COUNT, COUNT_UNIQUE, MEAN, MEDIAN", "blue")
        self.__ui.print_colored("Example:", "green")
        self.__ui.print_colored("  csvbro data.csv", "blue")
        self.__ui.print_colored("  csvbro data.csv SHOW account_id mobile ORDER_BY mobile ASC LIMIT 10", "blue")
        self.__ui.print_colored("  csvbro data.csv PIVOT lco_name mobile COUNT_UNIQUE ORDER_BY mobile DESC LIMIT 10", "blue")
        self.__ui.print_colored("  csvbro data.csv JSON ORDER_BY mobile DESC LIMIT 100", "blue")
        self.__ui.print_colored("  csvbro data.csv ORDER_BY mobile ASC LIMIT 5", "blue")

    def run(self, args):
        """Execute the main program logic."""
        self.__ui.display_logo()
        
        if len(args) < 2:
            self.__print_usage()
            sys.exit(1)
        
        self.__filename = args[1]
        self.__load_csv(self.__filename)
        
        if len(args) == 2:
            self.__show_dataframe()
        elif args[2].upper() == "SHOW":
            # Parse SHOW command
            i = 3
            columns = []
            sort_column = None
            sort_direction = 'ASC'
            limit = None
            while i < len(args):
                if args[i].upper() == "ORDER_BY":
                    if i + 1 >= len(args):
                        self.__ui.print_colored("Error: ORDER_BY requires <sort_column> [ASC|DESC]", "red")
                        self.__print_usage()
                        sys.exit(1)
                    sort_column = args[i + 1]
                    sort_direction = 'DESC'  # Default to DESC
                    i += 2
                    if i < len(args) and args[i].upper() in self.__valid_directions:
                        sort_direction = args[i].upper()
                        i += 1
                elif args[i].upper() == "LIMIT":
                    if i + 1 >= len(args):
                        self.__ui.print_colored("Error: LIMIT requires a positive integer", "red")
                        self.__print_usage()
                        sys.exit(1)
                    limit = args[i + 1]
                    i += 2
                else:
                    columns.append(args[i])
                    i += 1
            if not columns:
                self.__ui.print_colored("Error: SHOW command requires at least one column name", "red")
                self.__print_usage()
                sys.exit(1)
            self.__show_dataframe(columns, sort_column, sort_direction, limit)
        elif args[2].upper() == "PIVOT":
            # Parse PIVOT command
            if len(args) < 6:
                self.__ui.print_colored("Error: PIVOT requires <row> <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>]", "red")
                self.__print_usage()
                sys.exit(1)
            row, value, aggfunc = args[3], args[4], args[5].upper()
            sort_column = None
            sort_direction = 'ASC'
            limit = None
            i = 6
            while i < len(args):
                if args[i].upper() == "ORDER_BY":
                    if i + 1 >= len(args):
                        self.__ui.print_colored("Error: ORDER_BY requires <sort_column> [ASC|DESC]", "red")
                        self.__print_usage()
                        sys.exit(1)
                    sort_column = args[i + 1]
                    sort_direction = 'DESC'  # Default to DESC
                    i += 2
                    if i < len(args) and args[i].upper() in self.__valid_directions:
                        sort_direction = args[i].upper()
                        i += 1
                elif args[i].upper() == "LIMIT":
                    if i + 1 >= len(args):
                        self.__ui.print_colored("Error: LIMIT requires a positive integer", "red")
                        self.__print_usage()
                        sys.exit(1)
                    limit = args[i + 1]
                    i += 2
                else:
                    self.__ui.print_colored(f"Error: Unexpected argument '{args[i]}' after AGGFUNC", "red")
                    self.__print_usage()
                    sys.exit(1)
            self.__create_pivot_table(row, value, aggfunc, sort_column, sort_direction, limit)
        elif args[2].upper() == "JSON":
            # Parse JSON command
            i = 3
            columns = []
            sort_column = None
            sort_direction = 'ASC'
            limit = None
            while i < len(args):
                if args[i].upper() == "ORDER_BY":
                    if i + 1 >= len(args):
                        self.__ui.print_colored("Error: ORDER_BY requires <sort_column> [ASC|DESC]", "red")
                        self.__print_usage()
                        sys.exit(1)
                    sort_column = args[i + 1]
                    sort_direction = 'DESC'  # Default to DESC
                    i += 2
                    if i < len(args) and args[i].upper() in self.__valid_directions:
                        sort_direction = args[i].upper()
                        i += 1
                elif args[i].upper() == "LIMIT":
                    if i + 1 >= len(args):
                        self.__ui.print_colored("Error: LIMIT requires a positive integer", "red")
                        self.__print_usage()
                        sys.exit(1)
                    limit = args[i + 1]
                    i += 2
                else:
                    columns.append(args[i])
                    i += 1
            self.__output_json(columns if columns else None, sort_column, sort_direction, limit)
        elif args[2].upper() == "ORDER_BY":
            # Parse standalone ORDER_BY command
            if len(args) < 5:
                self.__ui.print_colored("Error: ORDER_BY requires <sort_column> <ASC|DESC> [LIMIT <n>]", "red")
                self.__print_usage()
                sys.exit(1)
            sort_column = args[3]
            sort_direction = args[4].upper()
            limit = None
            if sort_direction not in self.__valid_directions:
                self.__ui.print_colored(f"Error: Invalid sort direction '{sort_direction}'. Choose from ASC, DESC.", "red")
                sys.exit(1)
            if len(args) > 5:
                if args[5].upper() != "LIMIT":
                    self.__ui.print_colored(f"Error: Expected LIMIT after sort direction, got '{args[5]}'", "red")
                    self.__print_usage()
                    sys.exit(1)
                if len(args) < 7:
                    self.__ui.print_colored("Error: LIMIT requires a positive integer", "red")
                    self.__print_usage()
                    sys.exit(1)
                limit = args[6]
            self.__show_dataframe(sort_column=sort_column, sort_direction=sort_direction, limit=limit)
        elif args[2].upper() == "LIMIT":
            # Parse standalone LIMIT command
            if len(args) != 4:
                self.__ui.print_colored("Error: LIMIT requires a positive integer", "red")
                self.__print_usage()
                sys.exit(1)
            limit = args[3]
            self.__show_dataframe(limit=limit)
        else:
            self.__ui.print_colored(f"Error: Unknown command '{args[2]}'", "red")
            self.__print_usage()
            sys.exit(1)

if __name__ == "__main__":
    app = App()
    app.run(sys.argv)
