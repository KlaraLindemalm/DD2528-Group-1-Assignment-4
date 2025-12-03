"""
Grid data structure for robot navigation.
Implements a 1-indexed grid with Manhattan distance metrics.
"""

from typing import List, Tuple, Optional, Set
import logging

logger = logging.getLogger(__name__)


class Grid:
    """
    1-indexed grid data structure for robot navigation.
    Grid positions are numbered 1-42 as:
      1  2  3  4  5  6
      7  8  9 10 11 12
      13 14 15 16 17 18
      19 20 21 22 23 24
      25 26 27 28 29 30
      31 32 33 34 35 36
      37 38 39 40 41 42
    """

    def __init__(self, height: int, width: int):
        assert height > 0 and width > 0, "Grid dimensions must be positive"
        self.height = height
        self.width = width
        self.total_cells = height * width
        self.shelves: Set[int] = set()
        logger.info(
            f"Created grid with height={height}, width={width}, total cells={self.total_cells}"
        )

    def add_shelf(self, position: int) -> None:
        """Add a shelf (obstacle) at the given position"""
        assert self.is_valid_position(position), f"Position {position} is out of bounds"
        self.shelves.add(position)
        logger.info(f"Added shelf at position {position}")

    def remove_shelf(self, position: int) -> None:
        """Remove a shelf from the given position"""
        if position in self.shelves:
            self.shelves.remove(position)
            logger.info(f"Removed shelf at position {position}")

    def is_shelf(self, position: int) -> bool:
        """Check if a position contains a shelf"""
        return position in self.shelves

    def is_valid_position(self, position: int) -> bool:
        """Check if a position is valid (within grid bounds)"""
        return 1 <= position <= self.total_cells

    def is_walkable(self, position: int) -> bool:
        """Check if a position is walkable (valid and no shelf)"""
        return self.is_valid_position(position) and not self.is_shelf(position)

    def position_to_coords(self, position: int) -> Tuple[int, int]:
        """
        Convert 1-indexed position to (row, col) coordinates (0-indexed)
        Example: position 1 -> (0, 0), position 7 -> (1, 0)
        """
        assert self.is_valid_position(position), f"Position {position} is out of bounds"
        position_0indexed = position - 1
        row = position_0indexed // self.width
        col = position_0indexed % self.width
        return (row, col)

    def coords_to_position(self, row: int, col: int) -> int:
        """
        Convert (row, col) coordinates to 1-indexed position
        Example: (0, 0) -> position 1, (1, 0) -> position 7
        """
        assert (
            0 <= row < self.height and 0 <= col < self.width
        ), f"Coordinates ({row}, {col}) are out of bounds"
        return row * self.width + col + 1

    def manhattan_distance(self, pos1: int, pos2: int) -> int:
        """Calculate Manhattan distance between two positions"""
        assert self.is_valid_position(pos1), f"Position {pos1} is out of bounds"
        assert self.is_valid_position(pos2), f"Position {pos2} is out of bounds"

        r1, c1 = self.position_to_coords(pos1)
        r2, c2 = self.position_to_coords(pos2)
        distance = abs(r1 - r2) + abs(c1 - c2)
        return distance

    def get_neighbors(self, position: int) -> List[int]:
        """
        Get valid neighboring positions (up, down, left, right)
        No diagonal movement allowed
        """
        assert self.is_valid_position(position), f"Position {position} is out of bounds"

        row, col = self.position_to_coords(position)
        neighbors = []

        # Up (row - 1)
        if row > 0:
            neighbors.append(self.coords_to_position(row - 1, col))

        # Down (row + 1)
        if row < self.height - 1:
            neighbors.append(self.coords_to_position(row + 1, col))

        # Left (col - 1)
        if col > 0:
            neighbors.append(self.coords_to_position(row, col - 1))

        # Right (col + 1)
        if col < self.width - 1:
            neighbors.append(self.coords_to_position(row, col + 1))

        return neighbors

    def visualize(
        self, path: Optional[List[int]] = None, robot_pos: Optional[int] = None
    ) -> str:
        """Visualize the grid with shelves, path, and robot position"""
        result = []
        path_set = set(path) if path else set()

        for row in range(self.height):
            row_str = []
            for col in range(self.width):
                pos = self.coords_to_position(row, col)

                if pos == robot_pos:
                    row_str.append(" R ")
                elif self.is_shelf(pos):
                    row_str.append(" X ")
                elif pos in path_set:
                    row_str.append(" * ")
                else:
                    row_str.append(f"{pos:3d}")

            result.append("".join(row_str))

        return "\n".join(result)
