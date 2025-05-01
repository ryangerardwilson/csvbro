#!/usr/bin/env python3
import sys
from ui import UI
from csv_loader import CsvLoader
from dataframe_viewer import DataFrameViewer
from json_exporter import JsonExporter
from pivot_table_creator import PivotTableCreator
from command_parser import CommandParser

class App:
    def __init__(self):
        self.__ui = UI()
        self.__loader = CsvLoader(self.__ui)
        self.__viewer = DataFrameViewer(self.__ui)
        self.__json_exporter = JsonExporter(self.__ui)
        self.__pivot_creator = PivotTableCreator(self.__ui)
        self.__parser = CommandParser(self.__ui, self.__viewer, self.__json_exporter, self.__pivot_creator)

    def run(self, args: list):
        """Execute the main program logic."""
        self.__ui.display_logo()

        # Check if filename is provided
        if len(args) < 2:
            self.__parser.print_usage()
            sys.exit(1)

        # Load CSV file
        df = self.__loader.load_csv(args[1])

        # Parse and execute command
        self.__parser.parse_and_execute(args, df)

def main():
    app = App()
    app.run(sys.argv)

if __name__ == "__main__":
    main()
