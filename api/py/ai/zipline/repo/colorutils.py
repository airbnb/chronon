class Color:
    # colors chosen to be visible clearly on BOTH black and white terminals
    # change with caution
    NORMAL = '\033[0m'
    BOLD = '\033[1m'
    ITALIC = '\033[3m'
    UNDERLINE = '\033[4m'
    RED = '\033[38;5;160m'
    GREEN = '\033[38;5;28m'
    ORANGE = '\033[38;5;130m'
    YELLOW = '\u001b[33m'
    BLUE = '\033[38;5;27m'
    GREY = '\033[38;5;246m'
    HIGHLIGHT = BOLD+ITALIC+RED
