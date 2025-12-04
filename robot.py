"""
Base Robot class for grid navigation.
Provides movement capabilities with safety checks.
"""

from grid import Grid
import logging

logger = logging.getLogger(__name__)

"""
TODOs:
[] Battery simulation
    [X] Decrease battery on move
    [X] Recharge method
    [] Notify low battery
[X] Fetch method
[X] Put method
[] Message receiving
"""


class Robot:
    """Base robot class that navigates the grid with safety checks"""

    def __init__(self, grid: Grid, start_position: int):
        assert grid.is_valid_position(
            start_position
        ), f"Start position {start_position} is out of bounds"
        assert grid.is_walkable(
            start_position
        ), f"Start position {start_position} is blocked by a shelf"

        self.grid = grid
        self.position = start_position
        self.battery_level = 100
        self.rfid_held = None
        logger.info(f"Initialized at position {start_position}")

    def move_to(self, new_position: int) -> bool:
        """
        Move robot to a new position with safety checks
        Returns True if successful, False otherwise
        """
        assert self.grid.is_valid_position(
            new_position
        ), f"Target position {new_position} is out of bounds"

        # Check if positions are adjacent
        neighbors = self.grid.get_neighbors(self.position)
        assert (
            new_position in neighbors
        ), f"Position {new_position} is not adjacent to current position {self.position}"

        # Check for collision
        if self.grid.is_shelf(new_position):
            logger.warning(f"Cannot move to position {new_position}: shelf detected")
            return False

        if self.battery_level <= 0:
            logger.warning("Cannot move: battery depleted")
            return False

        old_position = self.position
        self.position = new_position

        distance = self.grid.manhattan_distance(old_position, new_position)
        self.battery_level -= distance

        logger.info(
            f"Moved {old_position} ➡ {new_position}\t[⚡️{self.battery_level}% left]"
        )
        return True

    def get_position(self) -> int:
        """Get the current position of the robot"""
        return self.position

    def recharge_battery(self):
        """Recharge the robot's battery to full"""
        self.battery_level = 100
        logger.info("🔋 Battery recharged to 100%")

    def get_battery_level(self) -> int:
        """Get the current battery level of the robot"""
        return self.battery_level

    def fetch_item(self, cb_pos: int, rfid: int) -> bool:
        """Fetch an item with the given RFID"""
        if (dist := self.grid.manhattan_distance(cb_pos, self.position)) != 1:
            logger.warning(
                f"Cannot put item, robot at {self.position}, not adjacent to CB at {cb_pos} (distance {dist})"
            )
        self.rfid_held = rfid
        logger.info(f"📦 Fetched item with RFID {rfid}")
        return True

    def put_item(self, shelf_pos: int, rfid: int) -> bool:
        """Put an item with the given RFID at the current position"""
        if (dist := self.grid.manhattan_distance(shelf_pos, self.position)) != 1:
            logger.warning(
                f"Cannot put item, robot at {self.position}, not adjacent to shelf at {shelf_pos} (distance {dist})"
            )
            return False
        if self.rfid_held != rfid:
            logger.warning(f"Cannot put item with RFID {rfid}: not held by robot")
            return False

        logger.info(f"📦 Put item with RFID {rfid} at position {self.position}")
        self.rfid_held = None
        return True

    def get_holding_item(self) -> bool:
        """Check if the robot is currently holding an item"""
        # Placeholder implementation
        return self.rfid_held is not None
