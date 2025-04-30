import sys
import time
import threading

class UI:
    def __init__(self):
        self.__HEADING_COLOR = '\033[92m'  # Bright green
        self.__CONTENT_COLOR = '\033[94m'  # Bright blue
        self.__ERROR_COLOR = '\033[91m'    # Bright red
        self.__RESET_COLOR = '\033[0m'     # Reset
        self.__BANNER = r"""
                  ___ _____   _____ ___  ___  
                 / __/ __\ \ / / _ ) _ \/ _ \ 
                | (__\__ \\ V /| _ \   / (_) |
                 \___|___/ \_/ |___/_|_\\___/                                                
    ╔╗ ┬ ┬  ╦═╗┬ ┬┌─┐┌┐┌  ╔═╗┌─┐┬─┐┌─┐┬─┐┌┬┐  ╦ ╦┬┬  ┌─┐┌─┐┌┐┌
    ╠╩╗└┬┘  ╠╦╝└┬┘├─┤│││  ║ ╦├┤ ├┬┘├─┤├┬┘ ││  ║║║││  └─┐│ ││││
    ╚═╝ ┴   ╩╚═ ┴ ┴ ┴┘└┘  ╚═╝└─┘┴└─┴ ┴┴└──┴┘  ╚╩╝┴┴─┘└─┘└─┘┘└┘
==================================================================
"""
        self.__spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']

    def display_logo(self):
        """Display the csvbro ASCII art logo with a typewriter effect."""
        # Split the banner into lines
        lines = self.__BANNER.split('\n')
        
        # Optionally clear the terminal screen (uncomment if needed)
        # print("\033[H\033[J", end="")  # ANSI escape to clear screen
        
        for line in lines:
            # Print the line character by character for typewriter effect
            current_line = ""
            for char in line:
                current_line += char
                # Clear the current line before printing
                sys.stdout.write(f"\r{self.__HEADING_COLOR}{current_line}{self.__RESET_COLOR}")
                sys.stdout.flush()
                time.sleep(0.001)  # Slower delay for smoother effect
            # Move to the next line after completing the current one
            sys.stdout.write("\n")
            sys.stdout.flush()

    def animate_loading(self, stop_event, message="Processing"):
        """Display a Braille spinner animation."""
        idx = 0
        while not stop_event.is_set():
            sys.stdout.write(f"\r{self.__HEADING_COLOR}{message} {self.__spinner[idx]}{self.__RESET_COLOR}")
            sys.stdout.flush()
            idx = (idx + 1) % len(self.__spinner)
            time.sleep(0.1)
        sys.stdout.write(f"\r{self.__HEADING_COLOR}{message} Done!{self.__RESET_COLOR}\n")
        sys.stdout.flush()

    def print_colored(self, text, color):
        """Print text in the specified color."""
        if color == "green":
            print(f"{self.__HEADING_COLOR}{text}{self.__RESET_COLOR}")
        elif color == "blue":
            print(f"{self.__CONTENT_COLOR}{text}{self.__RESET_COLOR}")
        elif color == "red":
            print(f"{self.__ERROR_COLOR}{text}{self.__RESET_COLOR}")
        else:
            print(text)
