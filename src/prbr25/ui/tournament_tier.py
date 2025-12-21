from pyfiglet import figlet_format

from prbr25.ui.utils import clear_screen


def display_tournament_tier(tournament_name: str, event_name: str, weight_dict: dict):
    clear_screen()
    print(f"{tournament_name}: {event_name}")
    art = get_char_art_tournament_weight(weight_dict)
    print(art)
    input("Press Enter to go back...")


def generate_custom_char_art(text: str, char: str) -> str:
    art = figlet_format(text, font="colossal")
    result = "".join(char if c not in (" ", "\n") else c for c in art)
    return result


def get_char_art_tournament_weight(weight_dict: dict) -> str:
    if weight_dict["Sign"]:
        return generate_custom_char_art(weight_dict["grade"], weight_dict["Sign"])
    else:
        return generate_custom_char_art(weight_dict["grade"], weight_dict["grade"])
