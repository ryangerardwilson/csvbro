import sys
from ui import UI
from dataframe_viewer import DataFrameViewer
from json_exporter import JsonExporter
from pivot_table_creator import PivotTableCreator

class CommandParser:
    def __init__(self, ui: UI, viewer: DataFrameViewer, json_exporter: JsonExporter, pivot_creator: PivotTableCreator):
        self.__ui = ui
        self.__viewer = viewer
        self.__json_exporter = json_exporter
        self.__pivot_creator = pivot_creator
        self.__valid_directions = {'ASC', 'DESC'}

    def print_usage(self):
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

    def parse_and_execute(self, args: list, df):
        """Parse command-line arguments and execute the appropriate command."""
        if len(args) < 2:
            self.print_usage()
            sys.exit(1)
        
        if len(args) == 2:
            self.__viewer.show_dataframe(df)
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
                        self.print_usage()
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
                        self.print_usage()
                        sys.exit(1)
                    limit = args[i + 1]
                    i += 2
                else:
                    columns.append(args[i])
                    i += 1
            if not columns:
                self.__ui.print_colored("Error: SHOW command requires at least one column name", "red")
                self.print_usage()
                sys.exit(1)
            self.__viewer.show_dataframe(df, columns, sort_column, sort_direction, limit)
        elif args[2].upper() == "PIVOT":
            # Parse PIVOT command
            if len(args) < 6:
                self.__ui.print_colored("Error: PIVOT requires <row> <value> <AGGFUNC> [ORDER_BY <sort_column> [ASC|DESC]] [LIMIT <n>]", "red")
                self.print_usage()
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
                        self.print_usage()
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
                        self.print_usage()
                        sys.exit(1)
                    limit = args[i + 1]
                    i += 2
                else:
                    self.__ui.print_colored(f"Error: Unexpected argument '{args[i]}' after AGGFUNC", "red")
                    self.print_usage()
                    sys.exit(1)
            self.__pivot_creator.create_pivot_table(df, row, value, aggfunc, sort_column, sort_direction, limit)
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
                        self.print_usage()
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
                        self.print_usage()
                        sys.exit(1)
                    limit = args[i + 1]
                    i += 2
                else:
                    columns.append(args[i])
                    i += 1
            self.__json_exporter.output_json(df, columns if columns else None, sort_column, sort_direction, limit)
        elif args[2].upper() == "ORDER_BY":
            # Parse standalone ORDER_BY command
            if len(args) < 5:
                self.__ui.print_colored("Error: ORDER_BY requires <sort_column> <ASC|DESC> [LIMIT <n>]", "red")
                self.print_usage()
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
                    self.print_usage()
                    sys.exit(1)
                if len(args) < 7:
                    self.__ui.print_colored("Error: LIMIT requires a positive integer", "red")
                    self.print_usage()
                    sys.exit(1)
                limit = args[6]
            self.__viewer.show_dataframe(df, sort_column=sort_column, sort_direction=sort_direction, limit=limit)
        elif args[2].upper() == "LIMIT":
            # Parse standalone LIMIT command
            if len(args) != 4:
                self.__ui.print_colored("Error: LIMIT requires a positive integer", "red")
                self.print_usage()
                sys.exit(1)
            limit = args[3]
            self.__viewer.show_dataframe(df, limit=limit)
        else:
            self.__ui.print_colored(f"Error: Unknown command '{args[2]}'", "red")
            self.print_usage()
            sys.exit(1)
