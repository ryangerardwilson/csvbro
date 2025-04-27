import pandas as pd
import sys
import threading
from ui import UI

class CsvLoader:
    def __init__(self, ui: UI):
        self.__ui = ui
        self.__df = None
        self.__filename = None

    def load_csv(self, filename: str) -> pd.DataFrame:
        """Load a CSV file into a pandas DataFrame."""
        self.__filename = filename
        try:
            stop_animation = threading.Event()
            animation_thread = threading.Thread(target=self.__ui.animate_loading, args=(stop_animation, f"Loading {filename}"))
            animation_thread.start()
            
            self.__df = pd.read_csv(filename)
            
            stop_animation.set()
            animation_thread.join()
            return self.__df
        except FileNotFoundError:
            self.__ui.print_colored(f"Error: File '{filename}' not found.", "red")
            sys.exit(1)
        except Exception as e:
            self.__ui.print_colored(f"Error loading CSV: {str(e)}", "red")
            sys.exit(1)

    @property
    def dataframe(self) -> pd.DataFrame:
        """Get the loaded DataFrame."""
        return self.__df

    @property
    def filename(self) -> str:
        """Get the loaded filename."""
        return self.__filename
