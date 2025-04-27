import pandas as pd
import sys
from ui import UI
import re
import ast

class WhereClauseHandler:
    def __init__(self, ui: UI):
        self.__ui = ui
        self.__operators = {
            '=': lambda x, y: x == y,
            '>': lambda x, y: x > float(y),
            '<': lambda x, y: x < float(y),
            '>=': lambda x, y: x >= float(y),
            '<=': lambda x, y: x <= float(y),
            '!=': lambda x, y: x != float(y),
            'LIKE': lambda x, y: x.str.contains(y, case=False, na=False)
        }

    def parse_and_apply(self, df: pd.DataFrame, where_clause: str) -> pd.DataFrame:
        """Parse and apply a WHERE clause with AND/OR logic to the DataFrame."""
        self.__ui.print_colored(f"Debug: Parsing WHERE clause: '{where_clause}'", "blue")
        if not where_clause:
            self.__ui.print_colored("Error: WHERE clause is empty.", "red")
            sys.exit(1)

        # Remove surrounding quotes if present
        where_clause = where_clause.strip()
        if where_clause.startswith('"') and where_clause.endswith('"'):
            where_clause = where_clause[1:-1]

        # Parse the condition (supports AND, OR, parentheses)
        try:
            condition = self._parse_condition(where_clause, df)
            filtered_df = df[condition]
            if filtered_df.empty:
                self.__ui.print_colored(f"Warning: No rows match the condition '{where_clause}'. Returning empty DataFrame.", "red")
            else:
                self.__ui.print_colored(f"Debug: Filtered DataFrame has {len(filtered_df)} rows.", "blue")
            return filtered_df
        except Exception as e:
            self.__ui.print_colored(f"Error applying WHERE clause: {str(e)}", "red")
            sys.exit(1)

    def _parse_condition(self, condition: str, df: pd.DataFrame) -> pd.Series:
        """Recursively parse a condition with AND/OR and evaluate it."""
        condition = condition.strip()

        # Handle parentheses
        if condition.startswith('(') and condition.endswith(')'):
            return self._parse_condition(condition[1:-1], df)

        # Split on AND/OR (outside parentheses)
        def split_logical(cond):
            parts = []
            current = ''
            paren_count = 0
            i = 0
            while i < len(cond):
                if cond[i] == '(':
                    paren_count += 1
                elif cond[i] == ')':
                    paren_count -= 1
                elif paren_count == 0 and cond[i:i+3].upper() in ('AND', 'OR '):
                    if current:
                        parts.append(current.strip())
                        current = ''
                    parts.append(cond[i:i+3].upper())
                    i += 3
                    continue
                current += cond[i]
                i += 1
            if current:
                parts.append(current.strip())
            return parts

        parts = split_logical(condition)
        if len(parts) == 1:
            # Single condition
            return self._evaluate_simple_condition(parts[0], df)

        # Combine conditions with AND/OR
        result = None
        operator = None
        for part in parts:
            if part in ('AND', 'OR'):
                operator = part
                continue
            sub_condition = self._parse_condition(part, df)
            if result is None:
                result = sub_condition
            elif operator == 'AND':
                result = result & sub_condition
            elif operator == 'OR':
                result = result | sub_condition

        return result

    def _evaluate_simple_condition(self, condition: str, df: pd.DataFrame) -> pd.Series:
        """Evaluate a simple condition (e.g., column > value)."""
        # Match column, operator, and value
        match = re.match(
            r"(\w+)\s*(=|>|<|>=|<=|!=|LIKE)\s*['\"]?([^'\"]+)['\"]?",
            condition.strip(),
            re.IGNORECASE
        )
        if not match:
            self.__ui.print_colored(
                f"Error: Invalid condition syntax. Expected 'column operator value' (e.g., column = value, column > 1.23, column LIKE substring). Got '{condition}'",
                "red"
            )
            sys.exit(1)

        column, operator, value = match.group(1), match.group(2), match.group(3)
        self.__ui.print_colored(f"Debug: Parsed simple condition - column: '{column}', operator: '{operator}', value: '{value}'", "blue")

        if column not in df.columns:
            self.__ui.print_colored(f"Error: Column '{column}' not found in DataFrame.", "red")
            sys.exit(1)

        if operator.upper() not in self.__operators:
            self.__ui.print_colored(f"Error: Invalid operator '{operator}'. Supported operators: {', '.join(self.__operators.keys())}", "red")
            sys.exit(1)

        # Debug: Print column info
        dtype = df[column].dtype
        min_val = df[column].min() if pd.api.types.is_numeric_dtype(df[column]) else "N/A"
        max_val = df[column].max() if pd.api.types.is_numeric_dtype(df[column]) else "N/A"
        self.__ui.print_colored(f"Debug: Column '{column}' - dtype: {dtype}, min: {min_val}, max: {max_val}", "blue")

        # Convert value for numeric comparisons
        if pd.api.types.is_numeric_dtype(df[column]) and operator.upper() != 'LIKE':
            try:
                value = float(value)
            except ValueError:
                self.__ui.print_colored(f"Error: Value '{value}' must be numeric for column '{column}' with numeric dtype.", "red")
                sys.exit(1)
        elif operator.upper() == 'LIKE' and not pd.api.types.is_string_dtype(df[column]):
            self.__ui.print_colored(f"Error: LIKE operator requires a string column, but '{column}' is of type {dtype}.", "red")
            sys.exit(1)

        # Apply the condition
        return self.__operators[operator.upper()](df[column], value)
