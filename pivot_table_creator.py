import pandas as pd
import sys
import threading
from ui import UI

class PivotTableCreator:
    def __init__(self, ui: UI):
        self.__ui = ui
        self.__valid_aggfuncs = {'SUM', 'COUNT', 'COUNT_UNIQUE', 'MEAN', 'MEDIAN'}
        self.__valid_directions = {'ASC', 'DESC'}

    def create_pivot_table(self, df: pd.DataFrame, row: str, value: str, aggfunc: str, column: str = None, sort_column: str = None, sort_direction: str = 'ASC', limit: str = None):
        """Create a pivot table from the DataFrame with specified row, optional column, value, aggregation, sorting, and limit."""
        try:
            # Validate column names
            for col_name in [row, value] + ([column] if column else []):
                if col_name not in df.columns:
                    self.__ui.print_colored(f"Error: Column '{col_name}' not found in DataFrame.", "red")
                    sys.exit(1)
            
            # Validate aggregation function
            if aggfunc not in self.__valid_aggfuncs:
                self.__ui.print_colored(f"Error: Invalid aggregation function '{aggfunc}'. Choose from {', '.join(self.__valid_aggfuncs)}.", "red")
                sys.exit(1)
            
            # Check if value column is numeric for SUM, MEAN, MEDIAN
            if aggfunc in {'SUM', 'MEAN', 'MEDIAN'} and not pd.api.types.is_numeric_dtype(df[value]):
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
            
            # Create pivot table with optional columns parameter
            pivot = pd.pivot_table(df, index=row, values=value, columns=column, aggfunc=agg_func, fill_value=0)
            
            # Handle sorting
            if sort_column:
                if column and sort_column not in pivot.columns:
                    self.__ui.print_colored(f"Error: Sort column '{sort_column}' not found in pivot table columns.", "red")
                    sys.exit(1)
                if not column and sort_column != value:
                    self.__ui.print_colored(f"Error: Sort column '{sort_column}' must be the value column '{value}' for pivot table without columns.", "red")
                    sys.exit(1)
                if sort_direction not in self.__valid_directions:
                    self.__ui.print_colored(f"Error: Invalid sort direction '{sort_direction}'. Choose from ASC, DESC.", "red")
                    sys.exit(1)
                ascending = sort_direction == 'ASC'
                if column:
                    pivot = pivot.sort_values(by=sort_column, ascending=ascending)
                else:
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
            
            self.__ui.print_colored(f"\nPivot Table ({aggfunc}{', Column: ' + column if column else ''}):", "green")
            self.__ui.print_colored("=" * 50, "green")
            self.__ui.print_colored(str(pivot), "blue")
            self.__ui.print_colored("=" * 50, "green")
            
            # Reset pandas display option
            pd.options.display.max_rows = original_max_rows
        except Exception as e:
            self.__ui.print_colored(f"Error creating pivot table: {str(e)}", "red")
            sys.exit(1)
