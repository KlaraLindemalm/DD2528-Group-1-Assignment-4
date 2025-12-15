"""
Dijkstra's pathfinding algorithm for robot navigation.
Finds the shortest path from start to goal avoiding obstacles.
"""

import heapq
from typing import List, Optional, Dict, Union
from robot import Robot
import logging

logger = logging.getLogger(__name__)


def find_path(start: Union[Robot, int], goal: Optional[int] = None, grid=None) -> Union[Optional[List[int]], Dict[int, List[int]]]:
    """
    Find shortest path using Dijkstra's algorithm.

    Args:
        start: Either a `Robot` instance (uses robot.position and robot.grid) or
               an integer start position (requires `grid` to be provided).
        goal: Target position to reach. If None, returns paths to all reachable positions.
        grid: Optional grid instance to use when `start` is an int.

    Returns:
        If goal is specified: List of positions from start to goal, or None if no path exists
        If goal is None: Dictionary mapping each reachable position to its path from start
    """
    # Resolve start position and grid
    if isinstance(start, Robot):
        start_pos = start.position
        grid_obj = start.grid
    else:
        start_pos = int(start)
        if grid is None:
            raise AssertionError("When calling find_path with a start position int, a grid must be provided")
        grid_obj = grid

    if goal is not None:
        assert grid_obj.is_valid_position(goal), f"Goal {goal} is out of bounds"

        if not grid_obj.is_walkable(goal):
            logger.error(f"Goal position {goal} is blocked by a shelf")
            return None

        logger.info(f"Running Dijkstra from {start_pos} to {goal}")
    else:
        logger.info(f"Running Dijkstra from {start_pos} to find all reachable positions")

    start = start_pos

    # Priority queue: (distance, position)
    pq = [(0, start)]
    distances: dict[int, int] = {start: 0}
    previous: dict[int, Optional[int]] = {start: None}
    visited: set[int] = set()

    while pq:
        current_dist, current = heapq.heappop(pq)

        if current in visited:
            continue

        visited.add(current)

        if goal is not None and current == goal:
            # Reconstruct path
            path = []
            pos: Optional[int] = goal
            while pos is not None:
                path.append(pos)
                pos = previous[pos]
            path.reverse()

            logger.info(f"Dijkstra found path of length {len(path)}: {path}")
            return path

        for neighbor in grid_obj.get_neighbors(current):
            if neighbor in visited or not grid_obj.is_walkable(neighbor):
                continue

            new_dist = current_dist + 1  # All moves have cost 1

            if neighbor not in distances or new_dist < distances[neighbor]:
                distances[neighbor] = new_dist
                previous[neighbor] = current
                heapq.heappush(pq, (new_dist, neighbor))

    if goal is not None:
        logger.warning(f"No path found from {start_pos} to {goal}")
        return None
    else:
        # Reconstruct paths to all reachable positions
        all_paths: Dict[int, List[int]] = {}
        for destination in visited:
            path = []
            pos: Optional[int] = destination
            while pos is not None:
                path.append(pos)
                pos = previous[pos]
            path.reverse()
            all_paths[destination] = path

        logger.info(f"Dijkstra found paths to {len(all_paths)} reachable positions")
        return all_paths


def navigate(robot: Robot, goal: int) -> bool:
    """
    Navigate robot to goal using Dijkstra's algorithm

    Args:
        robot: Robot instance to navigate
        goal: Target position

    Returns:
        True if goal reached, False otherwise
    """
    result = find_path(robot, goal)

    # Since we passed a goal, result should be Optional[List[int]]
    if not result or not isinstance(result, list):
        logger.error("Cannot navigate: no path found")
        return False

    path: List[int] = result

    # Execute the path
    for i in range(1, len(path)):
        next_pos = path[i]
        success = robot.move_to(next_pos)

        if not success:
            logger.error(f"Navigation failed at position {robot.position}")
            return False

    logger.info(f"Successfully navigated to goal at position {goal}")
    return True
