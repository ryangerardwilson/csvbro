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

            CCCCCCCCCCCCC   SSSSSSSSSSSSSSS VVVVVVVV           VVVVVVVV
         CCC::::::::::::C SS:::::::::::::::SV::::::V           V::::::V
       CC:::::::::::::::CS:::::SSSSSS::::::SV::::::V           V::::::V
      C:::::CCCCCCCC::::CS:::::S     SSSSSSSV::::::V           V::::::V
     C:::::C       CCCCCCS:::::S             V:::::V           V:::::V 
    C:::::C              S:::::S              V:::::V         V:::::V  
    C:::::C               S::::SSSS            V:::::V       V:::::V   
    C:::::C                SS::::::SSSSS        V:::::V     V:::::V    
    C:::::C                  SSS::::::::SS       V:::::V   V:::::V     
    C:::::C                     SSSSSS::::S       V:::::V V:::::V      
    C:::::C                          S:::::S       V:::::V:::::V       
     C:::::C       CCCCCC            S:::::S        V:::::::::V        
      C:::::CCCCCCCC::::CSSSSSSS     S:::::S         V:::::::V         
       CC:::::::::::::::CS::::::SSSSSS:::::S          V:::::V          
         CCC::::::::::::CS:::::::::::::::SS            V:::V           
            CCCCCCCCCCCCC SSSSSSSSSSSSSSS               VVV   

                                   (        )   
                               (   )\ )  ( /(   
                             ( )\ (()/(  )\())  
                             )((_) /(_))((_)\   
                            ((_)_ (_))    ((_)  
                             | _ )| _ \  / _ \  
                             | _ \|   / | (_) | 
                             |___/|_|_\  \___/  
                                                
            ╔╗ ┬ ┬  ╦═╗┬ ┬┌─┐┌┐┌  ╔═╗┌─┐┬─┐┌─┐┬─┐┌┬┐  ╦ ╦┬┬  ┌─┐┌─┐┌┐┌
            ╠╩╗└┬┘  ╠╦╝└┬┘├─┤│││  ║ ╦├┤ ├┬┘├─┤├┬┘ ││  ║║║││  └─┐│ ││││
            ╚═╝ ┴   ╩╚═ ┴ ┴ ┴┘└┘  ╚═╝└─┘┴└─┴ ┴┴└──┴┘  ╚╩╝┴┴─┘└─┘└─┘┘└┘
==================================================================================
"""
        self.__spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']

    def display_logo(self):
        """Display the csvbro ASCII art logo with a rapid typewriter effect."""
        current_line = ""
        for char in self.__BANNER:
            if char == '\n':
                print(f"{self.__HEADING_COLOR}{current_line}{self.__RESET_COLOR}")
                current_line = ""
                time.sleep(0.0005)  # Delay between lines
            else:
                current_line += char
                print(f"{self.__HEADING_COLOR}{current_line}{self.__RESET_COLOR}", end='\r')
                sys.stdout.flush()
                time.sleep(0.0005)  # Rapid typing effect for each character
        # Print the final line if not empty
        if current_line:
            print(f"{self.__HEADING_COLOR}{current_line}{self.__RESET_COLOR}")

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
