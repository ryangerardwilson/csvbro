import sys
from ui import UI
from dataframe_viewer import DataFrameViewer
from json_exporter import JsonExporter
from pivot_table_creator import PivotTableCreator
from where_clause_handler import WhereClauseHandler
import re

class CommandParser:
    def __init__(self, ui: UI, viewer: DataFrameViewer, json_exporter: JsonExporter, pivot_creator: PivotTableCreator):
        self.__ui = ui
        self.__viewer = viewer
        self.__json_exporter = json_exporter
        self.__pivot_creator = pivot_creator
        self.__where_handler = WhereClauseHandler(ui)
        self.__valid_directions = {'ASC', 'DESC'}
        self.__valid_aggfuncs = {'SUM', 'COUNT', 'COUNT_UNIQUE', 'MEAN', 'MEDIAN'}

    def print_usage(self):
        """Print usage instructions."""
        self.__ui.print_colored("Usage: csvbro <filename> [<command> [args]]", "green")
        self.__ui.print_colored("If no command is provided, the CSV content is displayed as a normal DataFrame (with pandas' default truncation).", "green")
        print()
        self.__ui.print_colored("Commands:", "green")
        self.__ui.print_colored("  SHOW <column1> [<column2> ...] [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE \"<condition>\"]  Display specified columns of the CSV", "blue")
        self.__ui.print_colored("  PIVOT <row> [<column>] <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE \"<condition>\"]  Create a pivot table (value must be numeric for SUM, MEAN, MEDIAN)", "blue")
        self.__ui.print_colored("  PIVOT DECILES(<column>[,IGNORE_OUTLIERS]) [<pivot_column>] <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE \"<condition>\"]  Create a decile analysis of <column> with aggregation of <value>", "blue")
        self.__ui.print_colored("  PIVOT PERCENTILES(<column>[,IGNORE_OUTLIERS]) [<pivot_column>] <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE \"<condition>\"]  Create a percentile analysis of <column> with aggregation of <value>", "blue")
        self.__ui.print_colored("  JSON [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>] [WHERE \"<condition>\"]  Output the DataFrame in JSON format", "blue")
        self.__ui.print_colored("  ORDER_BY <sort_column> <ASC|DESC> [LIMIT <n>] [WHERE \"<condition>\"]  Sort the entire DataFrame", "blue")
        self.__ui.print_colored("  LIMIT <n> [WHERE \"<condition>\"]  Limit the entire DataFrame to <n> rows", "blue")
        self.__ui.print_colored("  WHERE \"<condition>\" [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>]  Filter and display the DataFrame", "blue")
        self.__ui.print_colored("  Condition syntax: column <operator> value [AND|OR condition] (e.g., \"column1 > 0.3\", \"Col1 > 0.3 AND (Col2 > 0.5 OR Col9 > 1.2)\")", "blue")
        self.__ui.print_colored("  Operators: =, >, <, >=, <=, !=, LIKE (for string containment)", "blue")
        self.__ui.print_colored("  Note: Condition must be enclosed in double quotes in the shell command.", "blue")
        self.__ui.print_colored("  Aggregation functions: SUM, COUNT, COUNT_UNIQUE, MEAN, MEDIAN", "blue")
        print()
        self.__ui.print_colored("Example:", "green")
        self.__ui.print_colored("  csvbro data.csv", "blue")
        self.__ui.print_colored("  csvbro data.csv SHOW account_id mobile ORDER_BY mobile ASC LIMIT 10", "blue")
        self.__ui.print_colored("  csvbro data.csv PIVOT lco_name category/ThirdParty mobile COUNT_UNIQUE ORDER_BY mobile DESC LIMIT 10", "blue")
        self.__ui.print_colored("  csvbro data.csv PIVOT DECILES(splitter_efficacy_score,IGNORE_OUTLIERS) partner_id COUNT_UNIQUE ORDER_BY count_unique DESC", "blue")
        self.__ui.print_colored("  csvbro data.csv PIVOT DECILES(splitter_efficacy_score,IGNORE_OUTLIERS) tenure_bin partner_id COUNT_UNIQUE WHERE \"tenure_bin = '180+'\"", "blue")
        self.__ui.print_colored("  csvbro data.csv PIVOT PERCENTILES(unique_splitter_contribution,IGNORE_OUTLIERS) partner_id COUNT_UNIQUE ORDER_BY count_unique DESC", "blue")
        self.__ui.print_colored("  csvbro data.csv JSON ORDER_BY mobile DESC LIMIT 100", "blue")
        self.__ui.print_colored("  csvbro data.csv ORDER_BY mobile ASC LIMIT 5 WHERE \"mobile = '1234567890'\"", "blue")
        self.__ui.print_colored("  csvbro data.csv WHERE \"splitter_efficacy_score > 0.3\"", "blue")
        self.__ui.print_colored("  csvbro data.csv WHERE \"tenure_bin LIKE '180'\"", "blue")
        self.__ui.print_colored("  csvbro data.csv WHERE \"splitter_efficacy_score > 0.3 AND (splitter_count > 100 OR active_base_customer_count < 50)\"", "blue")

    def parse_and_execute(self, args: list, df):
        """Parse command-line arguments and execute the appropriate command."""
        if len(args) < 2:
            self.__ui.print_colored("Error: No filename provided.", "red")
            self.print_usage()
            sys.exit(1)
        
        if len(args) == 2:
            self.__viewer.show_dataframe(df)
            return
        
        command = args[2].upper()
        
        # Handle WHERE command immediately to capture the condition
        if command == "WHERE":
            if len(args) < 4:
                self.__ui.print_colored("Error: WHERE command requires a condition (e.g., \"column operator value\")", "red")
                self.print_usage()
                sys.exit(1)
            where_clause = ' '.join(args[3:])  # Capture the condition as a single string
            try:
                df = self.__where_handler.parse_and_apply(df, where_clause)
                self.__viewer.show_dataframe(df)
            except Exception as e:
                self.__ui.print_colored(f"Error: Failed to execute WHERE command: {str(e)}", "red")
                self.print_usage()
                sys.exit(1)
            return
        
        i = 3
        columns = []
        sort_column = None
        sort_direction = 'ASC'
        limit = None
        where_clause = None
        row = None
        value = None
        aggfunc = None
        pivot_column = None
        is_deciles = False
        is_percentiles = False
        decile_column = None
        percentile_column = None
        ignore_outliers = False

        try:
            if command == "PIVOT":
                if len(args) < 5:
                    self.__ui.print_colored("Error: PIVOT requires at least <row> <value> <AGGFUNC> or DECILES/PERCENTILES(<column>[,IGNORE_OUTLIERS]) [<pivot_column>] <value> <AGGFUNC>", "red")
                    self.print_usage()
                    sys.exit(1)
                row = args[3]
                i = 4
                
                if row.upper().startswith("DECILES("):
                    is_deciles = True
                    match = re.match(r"DECILES\(([^,)]+)(?:,IGNORE_OUTLIERS)?\)", row, re.IGNORECASE)
                    if not match:
                        self.__ui.print_colored("Error: Invalid DECILES syntax. Expected DECILES(<column>[,IGNORE_OUTLIERS]).", "red")
                        self.print_usage()
                        sys.exit(1)
                    decile_column = match.group(1)
                    ignore_outliers = ",IGNORE_OUTLIERS" in row.upper()
                elif row.upper().startswith("PERCENTILES("):
                    is_percentiles = True
                    match = re.match(r"PERCENTILES\(([^,)]+)(?:,IGNORE_OUTLIERS)?\)", row, re.IGNORECASE)
                    if not match:
                        self.__ui.print_colored("Error: Invalid PERCENTILES syntax. Expected PERCENTILES(<column>[,IGNORE_OUTLIERS]).", "red")
                        self.print_usage()
                        sys.exit(1)
                    percentile_column = match.group(1)
                    ignore_outliers = ",IGNORE_OUTLIERS" in row.upper()
                
                if is_deciles or is_percentiles:
                    if i >= len(args):
                        self.__ui.print_colored(f"Error: PIVOT {'DECILES' if is_deciles else 'PERCENTILES'} requires <value> <AGGFUNC> or [<pivot_column>] <value> <AGGFUNC>", "red")
                        self.print_usage()
                        sys.exit(1)
                    if i + 1 < len(args) and args[i + 1].upper() in self.__valid_aggfuncs:
                        value = args[i]
                        aggfunc = args[i + 1].upper()
                        i += 2
                    else:
                        pivot_column = args[i]
                        i += 1
                        if i >= len(args):
                            self.__ui.print_colored(f"Error: PIVOT {'DECILES' if is_deciles else 'PERCENTILES'} requires <value> <AGGFUNC> after <pivot_column>", "red")
                            self.print_usage()
                            sys.exit(1)
                        value = args[i]
                        i += 1
                        if i >= len(args):
                            self.__ui.print_colored(f"Error: PIVOT {'DECILES' if is_deciles else 'PERCENTILES'} requires <AGGFUNC> after <value>", "red")
                            self.print_usage()
                            sys.exit(1)
                        aggfunc = args[i].upper()
                        i += 1
                else:
                    if i + 1 < len(args) and args[i + 1].upper() not in self.__valid_aggfuncs and args[i + 1].upper() not in {'ORDER_BY', 'LIMIT', 'WHERE'}:
                        pivot_column = args[i]
                        i += 1
                    if i >= len(args):
                        self.__ui.print_colored("Error: PIVOT requires <value> after <row> [<column>]", "red")
                        self.print_usage()
                        sys.exit(1)
                    value = args[i]
                    i += 1
                    if i >= len(args):
                        self.__ui.print_colored("Error: PIVOT requires <AGGFUNC> after <value>", "red")
                        self.print_usage()
                        sys.exit(1)
                    aggfunc = args[i].upper()
                    i += 1
                if aggfunc not in self.__valid_aggfuncs:
                    self.__ui.print_colored(f"Error: Invalid aggregation function '{aggfunc}'. Choose from {', '.join(self.__valid_aggfuncs)}.", "red")
                    self.print_usage()
                    sys.exit(1)
            
            # Parse remaining arguments (ORDER_BY, LIMIT, WHERE)
            while i < len(args):
                if args[i].upper() == "ORDER_BY":
                    if i + 1 >= len(args):
                        self.__ui.print_colored("Error: ORDER_BY requires <sort_column> [ASC|DESC]", "red")
                        self.print_usage()
                        sys.exit(1)
                    sort_column = args[i + 1]
                    sort_direction = 'ASC'
                    i += 2
                    if i < len(args) and args[i].upper() in self.__valid_directions:
                        sort_direction = args[i].upper()
                        i += 1
                elif args[i].upper() == "LIMIT":
                    if i + 1 >= len(args):
                        self.__ui.print_colored("Error: LIMIT requires a positive integer", "red")
                        self.print_usage()
                        sys.exit(1)
                    limit = args[i + 1]
                    i += 2
                elif args[i].upper() == "WHERE":
                    if i + 1 >= len(args):
                        self.__ui.print_colored("Error: WHERE requires a condition (e.g., \"column operator value\")", "red")
                        self.print_usage()
                        sys.exit(1)
                    where_clause = args[i + 1]
                    i += 2
                elif command in {"SHOW", "JSON"}:
                    columns.append(args[i])
                    i += 1
                else:
                    self.__ui.print_colored(f"Error: Unexpected argument '{args[i]}' in {command} command", "red")
                    self.print_usage()
                    sys.exit(1)
            
            # Apply WHERE clause if present
            if where_clause:
                df = self.__where_handler.parse_and_apply(df, where_clause)
            
            # Execute the command
            if command == "SHOW":
                if not columns:
                    self.__ui.print_colored("Error: SHOW command requires at least one column name", "red")
                    self.print_usage()
                    sys.exit(1)
                self.__viewer.show_dataframe(df, columns, sort_column, sort_direction, limit)
            elif command == "PIVOT":
                self.__pivot_creator.create_pivot_table(df, row, value, aggfunc, pivot_column, sort_column, sort_direction, limit, is_deciles, decile_column, ignore_outliers, is_percentiles, percentile_column)
            elif command == "JSON":
                self.__json_exporter.output_json(df, columns if columns else None, sort_column, sort_direction, limit)
            elif command == "ORDER_BY":
                if not sort_column or sort_direction not in self.__valid_directions:
                    self.__ui.print_colored("Error: ORDER_BY requires <sort_column> <ASC|DESC> [LIMIT <n>]", "red")
                    self.print_usage()
                    sys.exit(1)
                self.__viewer.show_dataframe(df, sort_column=sort_column, sort_direction=sort_direction, limit=limit)
            elif command == "LIMIT":
                if not limit:
                    self.__ui.print_colored("Error: LIMIT requires a positive integer", "red")
                    self.print_usage()
                    sys.exit(1)
                self.__viewer.show_dataframe(df, limit=limit)
            else:
                self.__ui.print_colored(f"Error: Unknown command '{command}'", "red")
                self.print_usage()
                sys.exit(1)
        
        except Exception as e:
            self.__ui.print_colored(f"Error: Failed to parse or execute command: {str(e)}", "red")
            self.print_usage()
            sys.exit(1)
