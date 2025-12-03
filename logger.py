"""
Enhanced color-coded logging system for robot navigation.
Integrates with Python's logging module for organized, colored output.
"""

import logging
import sys


class Colors:
    """ANSI color codes for terminal output"""

    # Component colors
    GRID = "\033[96m"      # Cyan
    ROBOT = "\033[94m"     # Blue
    DIJKSTRA = "\033[95m"  # Magenta
    BUG2 = "\033[92m"      # Green
    MAIN = "\033[90m"      # Gray

    # Level colors
    DEBUG = "\033[37m"     # White
    INFO = "\033[97m"      # Bright White
    WARNING = "\033[93m"   # Yellow
    ERROR = "\033[91m"     # Red
    CRITICAL = "\033[41m"  # Red background

    # Styles
    BOLD = "\033[1m"
    DIM = "\033[2m"
    UNDERLINE = "\033[4m"
    RESET = "\033[0m"

    # Special
    SUCCESS = "\033[92m"   # Green
    PATH = "\033[35m"      # Purple


class ColoredFormatter(logging.Formatter):
    """Custom formatter with color coding based on level and module"""

    # Module name to color mapping
    MODULE_COLORS = {
        'grid': Colors.GRID,
        'robot': Colors.ROBOT,
        'dijkstra': Colors.DIJKSTRA,
        'bug2': Colors.BUG2,
        'main': Colors.MAIN,
        '__main__': Colors.MAIN,
    }

    # Log level to color mapping
    LEVEL_COLORS = {
        'DEBUG': Colors.DEBUG,
        'INFO': Colors.INFO,
        'WARNING': Colors.WARNING,
        'ERROR': Colors.ERROR,
        'CRITICAL': Colors.CRITICAL,
    }

    def __init__(self, fmt: str | None = None, datefmt: str | None = None):
        super().__init__(fmt, datefmt)

    def format(self, record: logging.LogRecord) -> str:
        # Get module color
        module_name = record.name.split('.')[-1]
        module_color = self.MODULE_COLORS.get(module_name, Colors.INFO)

        # Get level color
        level_color = self.LEVEL_COLORS.get(record.levelname, Colors.INFO)

        # Create colored components
        timestamp = f"{Colors.DIM}{self.formatTime(record, self.datefmt or '%Y-%m-%d %H:%M:%S')}{Colors.RESET}"
        level = f"{level_color}{record.levelname:8s}{Colors.RESET}"
        module = f"{module_color}{record.name:12s}{Colors.RESET}"

        # Special handling for specific message patterns
        message = record.getMessage()

        # Highlight special keywords
        if "SUCCESS" in message or "Successfully" in message or "reached goal" in message:
            message = f"{Colors.SUCCESS}{Colors.BOLD}{message}{Colors.RESET}"
        elif "Path found" in message or "path:" in message:
            message = f"{Colors.PATH}{message}{Colors.RESET}"
        elif "FAILED" in message or "blocked" in message or "cannot" in message.lower():
            message = f"{Colors.ERROR}{message}{Colors.RESET}"
        elif "Obstacle detected" in message:
            message = f"{Colors.WARNING}{Colors.BOLD}{message}{Colors.RESET}"
        elif "Moving toward" in message or "moved from" in message:
            message = f"{module_color}{message}{Colors.RESET}"
        else:
            message = f"{module_color}{message}{Colors.RESET}"

        # Format final output
        return f"{timestamp} | {level} | {module} | {message}"


def configure_root_logger(level: int = logging.INFO):
    """
    Configure the root logger with color coding

    Args:
        level: Logging level for all loggers
    """
    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    root_logger.setLevel(level)

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    # Create and set formatter
    formatter = ColoredFormatter(
        fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    handler.setFormatter(formatter)

    # Add handler to root logger
    root_logger.addHandler(handler)
