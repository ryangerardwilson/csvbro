import pandas as pd
import sys
import threading
from ui import UI
import numpy as np

class PivotTableCreator:
    def __init__(self, ui: UI):
        self.__ui = ui
        self.__valid_aggfuncs = {'SUM', 'COUNT', 'COUNT_UNIQUE', 'MEAN', 'MEDIAN'}
        self.__valid_directions = {'ASC', 'DESC'}

    def compute_deciles(self, df: pd.DataFrame, column: str, ignore_outliers: bool = False) -> tuple:
        """Assign decile bins and return DataFrame with decile labels and bin ranges, optionally ignoring outliers."""
        try:
            if column not in df.columns:
                self.__ui.print_colored(f"Error: Column '{column}' not found in DataFrame.", "red")
                sys.exit(1)
            if not pd.api.types.is_numeric_dtype(df[column]):
                self.__ui.print_colored(f"Error: Decile column '{column}' must be numeric.", "red")
                sys.exit(1)
            
            # Check for missing values
            if df[column].isna().any():
                self.__ui.print_colored(f"Warning: Column '{column}' contains missing values. Dropping them for decile computation.", "red")
                df = df.dropna(subset=[column]).copy()
            
            # Filter outliers if requested
            if ignore_outliers:
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                original_len = len(df)
                df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)].copy()
                filtered_len = len(df)
                self.__ui.print_colored(f"Outlier removal: {original_len - filtered_len} rows removed as outliers (values < {lower_bound:.4f} or > {upper_bound:.4f}).", "blue")
                if filtered_len == 0:
                    self.__ui.print_colored(f"Error: No data remains after removing outliers.", "red")
                    sys.exit(1)
            
            # Debug: Inspect the column
            unique_count = df[column].nunique()
            total_rows = len(df)
            value_counts = df[column].value_counts()
            
            if unique_count < 2:
                self.__ui.print_colored(f"Error: Column '{column}' has too few unique values ({unique_count}) to compute deciles.", "red")
                sys.exit(1)
            
            # Use pd.cut for equal-width bins
            num_bins = min(10, unique_count)
            if num_bins < 10:
                self.__ui.print_colored(f"Warning: Only {num_bins} bins possible due to limited unique values in '{column}'.", "red")
            
            try:
                # Compute explicit bin edges
                min_val = df[column].min()
                max_val = df[column].max()
                bin_edges = np.linspace(min_val, max_val, num_bins + 1)
                # Adjust the last edge to include max_val
                bin_edges[-1] = max_val + 1e-10  # Small increment to include max value
                df['decile'] = pd.cut(df[column], bins=bin_edges, labels=[f'D{i+1}' for i in range(num_bins)], include_lowest=True)
                
                # Create range labels
                ranges = [f"[{bin_edges[i]:.4f}, {bin_edges[i+1]:.4f})" for i in range(len(bin_edges)-1)]
                range_df = pd.DataFrame({
                    'decile': [f'D{i+1}' for i in range(num_bins)],
                    'range': ranges
                })
                # Combine decile and range for display
                range_df['formatted_decile'] = range_df.apply(lambda x: f"{x['decile']} {x['range']}", axis=1)
            except ValueError as e:
                self.__ui.print_colored(f"Error: Failed to compute bins for '{column}'. Reason: {str(e)}", "red")
                sys.exit(1)
            
            return df, range_df
        except Exception as e:
            self.__ui.print_colored(f"Error computing deciles: {str(e)}", "red")
            sys.exit(1)

    def compute_percentiles(self, df: pd.DataFrame, column: str, ignore_outliers: bool = False) -> tuple:
        """Assign percentile bins (P0, P10, ..., P90) using equal-count binning and return DataFrame with percentile labels and bin ranges, optionally ignoring outliers."""
        try:
            if column not in df.columns:
                self.__ui.print_colored(f"Error: Column '{column}' not found in DataFrame.", "red")
                sys.exit(1)
            if not pd.api.types.is_numeric_dtype(df[column]):
                self.__ui.print_colored(f"Error: Percentile column '{column}' must be numeric.", "red")
                sys.exit(1)
            
            # Check for missing values
            if df[column].isna().any():
                self.__ui.print_colored(f"Warning: Column '{column}' contains missing values. Dropping them for percentile computation.", "red")
                df = df.dropna(subset=[column]).copy()
            
            # Filter outliers if requested
            if ignore_outliers:
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                original_len = len(df)
                df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)].copy()
                filtered_len = len(df)
                self.__ui.print_colored(f"Outlier removal: {original_len - filtered_len} rows removed as outliers (values < {lower_bound:.4f} or > {upper_bound:.4f}).", "blue")
                if filtered_len == 0:
                    self.__ui.print_colored(f"Error: No data remains after removing outliers.", "red")
                    sys.exit(1)
            
            # Debug: Inspect the column
            unique_count = df[column].nunique()
            total_rows = len(df)
            value_counts = df[column].value_counts()
            
            if unique_count < 10:
                self.__ui.print_colored(f"Error: Column '{column}' has too few unique values ({unique_count}) to compute 10 percentiles.", "red")
                sys.exit(1)
            
            try:
                # Add small random noise to break ties among duplicates
                noise = np.random.uniform(-1e-6, 1e-6, size=len(df))
                df[f'{column}_noised'] = df[column] + noise
                
                # Compute percentile bins using pd.qcut
                df['percentile'], bin_edges = pd.qcut(
                    df[f'{column}_noised'], 
                    q=10, 
                    labels=[f'P{i*10}' for i in range(10)], 
                    retbins=True
                )
                
                # Drop the noised column
                df = df.drop(columns=[f'{column}_noised'])
                
                # Create range labels based on original data
                ranges = [f"[{bin_edges[i]:.4f}, {bin_edges[i+1]:.4f})" for i in range(len(bin_edges)-1)]
                range_df = pd.DataFrame({
                    'percentile': [f'P{i*10}' for i in range(10)],
                    'range': ranges
                })
                # Combine percentile and range for display
                range_df['formatted_percentile'] = range_df.apply(lambda x: f"{x['percentile']} {x['range']}", axis=1)
            except ValueError as e:
                self.__ui.print_colored(f"Error: Failed to compute bins for '{column}'. Reason: {str(e)}", "red")
                self.__ui.print_colored(f"Debug: Column stats - min: {df[column].min()}, max: {df[column].max()}, unique values: {unique_count}", "blue")
                sys.exit(1)
            
            return df, range_df
        except Exception as e:
            self.__ui.print_colored(f"Error computing percentiles: {str(e)}", "red")
            sys.exit(1)

    def create_pivot_table(self, df: pd.DataFrame, row: str, value: str, aggfunc: str, column: str = None, sort_column: str = None, sort_direction: str = 'ASC', limit: str = None, is_deciles: bool = False, decile_column: str = None, ignore_outliers: bool = False, is_percentiles: bool = False, percentile_column: str = None):
        """Create a pivot table, decile-based, or percentile-based aggregation from the DataFrame, optionally ignoring outliers."""
        try:
            # Handle deciles or percentiles case
            if is_deciles or is_percentiles:
                target_column = decile_column if is_deciles else percentile_column
                bin_type = 'decile' if is_deciles else 'percentile'
                if target_column not in df.columns:
                    self.__ui.print_colored(f"Error: {bin_type.capitalize()} column '{target_column}' not found in DataFrame.", "red")
                    sys.exit(1)
                if value not in df.columns:
                    self.__ui.print_colored(f"Error: Value column '{value}' not found in DataFrame.", "red")
                    sys.exit(1)
                if aggfunc not in self.__valid_aggfuncs:
                    self.__ui.print_colored(f"Error: Invalid aggregation function '{aggfunc}'. Choose from {', '.join(self.__valid_aggfuncs)}.", "red")
                    sys.exit(1)
                if aggfunc in {'SUM', 'MEAN', 'MEDIAN'} and not pd.api.types.is_numeric_dtype(df[value]):
                    self.__ui.print_colored(f"Error: Value column '{value}' must be numeric for {aggfunc} aggregation.", "red")
                    sys.exit(1)
                
                stop_animation = threading.Event()
                animation_thread = threading.Thread(target=self.__ui.animate_loading, args=(stop_animation, f"Creating {bin_type} analysis"))
                animation_thread.start()
                
                # Compute bins and get range information
                if is_deciles:
                    df, range_df = self.compute_deciles(df, decile_column, ignore_outliers)
                else:
                    df, range_df = self.compute_percentiles(df, percentile_column, ignore_outliers)
                
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
                
                # Group by bin and optionally pivot_column
                if column:
                    if column not in df.columns:
                        self.__ui.print_colored(f"Error: Pivot column '{column}' not found in DataFrame.", "red")
                        sys.exit(1)
                    pivot = pd.pivot_table(df, index=bin_type, columns=column, values=value, aggfunc=agg_func, fill_value=0, observed=False)
                    pivot = pivot.reset_index()
                    # Merge with range information to get formatted labels
                    pivot = pivot.merge(range_df[[bin_type, f'formatted_{bin_type}']], on=bin_type, how='left')
                    pivot[bin_type] = pivot[f'formatted_{bin_type}']
                    pivot = pivot.drop(columns=[f'formatted_{bin_type}'])
                else:
                    # No pivot_column: aggregate by bin only
                    pivot = df.groupby(bin_type, observed=False)[value].agg(agg_func).reset_index(name=aggfunc.lower())
                    # Merge with range information to get formatted labels
                    pivot = pivot.merge(range_df[[bin_type, f'formatted_{bin_type}']], on=bin_type, how='left')
                    pivot[bin_type] = pivot[f'formatted_{bin_type}']
                    pivot = pivot.drop(columns=[f'formatted_{bin_type}'])
                
                # Handle sorting
                if sort_column:
                    if sort_column not in pivot.columns:
                        self.__ui.print_colored(f"Error: Sort column '{sort_column}' not found in result.", "red")
                        sys.exit(1)
                    if sort_direction not in self.__valid_directions:
                        self.__ui.print_colored(f"Error: Invalid sort direction '{sort_direction}'. Choose from ASC, DESC.", "red")
                        sys.exit(1)
                    ascending = sort_direction == 'ASC'
                    # Sort by sort_column and bin_order to ensure consistent ordering
                    pivot[f'{bin_type}_order'] = pivot[bin_type].str.extract(r'[DP](\d+)')[0].astype(int)
                    pivot = pivot.sort_values(by=[sort_column, f'{bin_type}_order'], ascending=[ascending, True]).reset_index(drop=True)
                    pivot = pivot.drop(columns=f'{bin_type}_order')
                else:
                    # Default to natural order (D1 to D10 or P0 to P90)
                    pivot[f'{bin_type}_order'] = pivot[bin_type].str.extract(r'[DP](\d+)')[0].astype(int)
                    pivot = pivot.sort_values(by=f'{bin_type}_order').reset_index(drop=True).drop(columns=f'{bin_type}_order')
                
                # Handle limit
                if limit is not None:
                    try:
                        limit = int(limit)
                        if limit <= 0:
                            raise ValueError
                        pivot = pivot.iloc[:limit]
                    except ValueError:
                        self.__ui.print_colored(f"Error: LIMIT must be a positive integer, got '{limit}'", "red")
                        sys.exit(1)
                
                # Display results
                original_max_rows = pd.options.display.max_rows
                pd.options.display.max_rows = None
                self.__ui.print_colored(f"\n{bin_type.capitalize()} Analysis ({aggfunc} of {value} by {bin_type}s of {target_column}{' with outliers ignored' if ignore_outliers else ''}{', Column: ' + column if column else ''}):", "green")
                self.__ui.print_colored("=" * 50, "green")
                self.__ui.print_colored(str(pivot), "blue")
                self.__ui.print_colored("=" * 50, "green")
                pd.options.display.max_rows = original_max_rows
                
                # Stop animation with timeout
                stop_animation.set()
                animation_thread.join(timeout=1.0)
                if animation_thread.is_alive():
                    self.__ui.print_colored("Warning: Animation thread did not terminate cleanly.", "red")
                
                return
            
            # Existing pivot table logic
            for col_name in [row, value] + ([column] if column else []):
                if col_name not in df.columns:
                    self.__ui.print_colored(f"Error: Column '{col_name}' not found in DataFrame.", "red")
                    sys.exit(1)
            
            if aggfunc not in self.__valid_aggfuncs:
                self.__ui.print_colored(f"Error: Invalid aggregation function '{aggfunc}'. Choose from {', '.join(self.__valid_aggfuncs)}.", "red")
                sys.exit(1)
            
            if aggfunc in {'SUM', 'MEAN', 'MEDIAN'} and not pd.api.types.is_numeric_dtype(df[value]):
                self.__ui.print_colored(f"Error: Value column '{value}' must be numeric for {aggfunc} aggregation.", "red")
                sys.exit(1)
            
            stop_animation = threading.Event()
            animation_thread = threading.Thread(target=self.__ui.animate_loading, args=(stop_animation, "Creating pivot table"))
            animation_thread.start()
            
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
            
            pivot = pd.pivot_table(df, index=row, values=value, columns=column, aggfunc=agg_func, fill_value=0)
            
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
            
            # Stop animation with timeout
            stop_animation.set()
            animation_thread.join(timeout=1.0)
            if animation_thread.is_alive():
                self.__ui.print_colored("Warning: Animation thread did not terminate cleanly.", "red")
            
            original_max_rows = pd.options.display.max_rows
            pd.options.display.max_rows = None
            
            self.__ui.print_colored(f"\nPivot Table ({aggfunc}{', Column: ' + column if column else ''}):", "green")
            self.__ui.print_colored("=" * 50, "green")
            self.__ui.print_colored(str(pivot), "blue")
            self.__ui.print_colored("=" * 50, "green")
            
            pd.options.display.max_rows = original_max_rows
        except Exception as e:
            self.__ui.print_colored(f"Error creating pivot table: {str(e)}", "red")
            sys.exit(1)
