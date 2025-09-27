import os
import shutil


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def print_centered(text):
    width = shutil.get_terminal_size().columns
    print(text.center(width))
