"""
Bug2 algorithm for robot navigation with dynamic obstacle avoidance.
Implements boundary following when obstacles are encountered.
"""

from typing import List, Set, Optional, Tuple
from robot import Robot
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class State(Enum):
    DIRECT = 1
    BOUNDARY = 2


def bug2_navigate(
    robot: Robot, goal: int, dynamic_obstacles: Optional[Set[int]] = None
) -> bool:
    """
    Navigate to goal using Bug2 algorithm with dynamic obstacle detection.

    Bug2 algorithm:
    1. Move toward goal following the m-line (direct Manhattan distance path)
    2. If obstacle encountered, record hit point and enter boundary following mode
    3. Follow obstacle boundary until m-line is reached at a point closer to goal
    4. Resume moving toward goal

    Args:
        robot: Robot instance to navigate
        goal: Target position
        dynamic_obstacles: Set of positions with dynamic obstacles

    Returns:
        True if goal reached, False if path blocked
    """
    assert robot.grid.is_valid_position(goal), f"Goal {goal} is out of bounds"

    if dynamic_obstacles is None:
        dynamic_obstacles = set()

    logger.info(f"Starting Bug2 navigation from {robot.position} to {goal}")
    logger.info(f"Dynamic obstacles at: {dynamic_obstacles}")

    max_iterations = robot.grid.total_cells * 4  # Allow for circumnavigation
    iteration = 0

    # M-line distance function (Manhattan distance to goal)
    def get_m_line_distance(pos: int) -> int:
        return robot.grid.manhattan_distance(pos, goal)

    # Check if position is blocked
    def is_blocked(pos: int) -> bool:
        return (
            not robot.grid.is_valid_position(pos)
            or not robot.grid.is_walkable(pos)
            or pos in dynamic_obstacles
        )

    # State: either DIRECT (following m-line) or BOUNDARY (wall-following)
    state = State.DIRECT
    hit_point_distance = 1_000_000  # Large initial value
    visited_in_boundary = set()

    while robot.position != goal and iteration < max_iterations:
        iteration += 1
        current_m_distance = get_m_line_distance(robot.position)

        # Check if we've reached the goal
        if robot.position == goal:
            return True

        # Get all valid neighbors
        neighbors = robot.grid.get_neighbors(robot.position)

        if state == State.DIRECT:
            # DIRECT MODE: Move toward goal along m-line
            # Sort neighbors by Manhattan distance to goal
            neighbors_by_distance = [
                (n, get_m_line_distance(n)) for n in neighbors if not is_blocked(n)
            ]
            neighbors_by_distance.sort(key=lambda x: x[1])

            if not neighbors_by_distance:
                logger.error("No valid neighbors in direct mode, path blocked")
                return False

            best_neighbor, best_distance = neighbors_by_distance[0]

            # Check if we're making progress toward goal
            if best_distance < current_m_distance:
                # Good progress, move toward goal
                logger.info(f"Direct mode - moving to {best_neighbor}")
                success = robot.move_to(best_neighbor)
                if not success:
                    logger.error(f"Failed to move to {best_neighbor} in direct mode")
                    return False
            else:
                # We've hit an obstacle. Enter boundary following mode
                logger.info(
                    f"Obstacle detected at m-distance {current_m_distance}. Entering boundary mode."
                )

                state = State.BOUNDARY
                hit_point_distance = current_m_distance
                visited_in_boundary = {robot.position}

                # Start wall-following by picking first available neighbor
                # ? REVIEW, should we pick the best neighbor instead?
                valid_neighbors = [n for n in neighbors if not is_blocked(n)]
                if not valid_neighbors:
                    logger.error("No valid neighbors when entering boundary mode")
                    return False

                success = robot.move_to(valid_neighbors[0])
                if not success:
                    logger.error(
                        f"Failed to move to {valid_neighbors[0]} when entering boundary mode"
                    )
                    return False
                logger.info(f"Boundary mode - initial move to {valid_neighbors[0]}")
                visited_in_boundary.add(valid_neighbors[0])

        else:  # state == State.BOUNDARY
            # Check leave condition: back on m-line and closer to goal than hit point
            if current_m_distance < hit_point_distance:
                logger.info(
                    f"Leave point found! m-distance {current_m_distance} < {hit_point_distance}"
                )
                logger.info("Returning to direct mode")
                state = State.DIRECT
                hit_point_distance = 1_000_000  # Reset to large initial value
                visited_in_boundary = set()
                continue

            # Wall-following: try to keep obstacle on right side
            # Get valid neighbors for wall-following
            valid_neighbors = [n for n in neighbors if not is_blocked(n)]

            if not valid_neighbors:
                logger.error("No valid neighbors in boundary mode, path blocked")
                return False

            # Choose next position for wall-following
            # Prefer positions that haven't been visited recently (avoid loops)
            unvisited = [n for n in valid_neighbors if n not in visited_in_boundary]

            if unvisited:
                # Prefer unvisited neighbors closest to the goal
                next_pos = min(unvisited, key=get_m_line_distance)
            else:
                # All neighbors visited, pick the one closest to goal
                next_pos = min(valid_neighbors, key=get_m_line_distance)

                # Check if we're stuck in a loop
                if len(visited_in_boundary) > robot.grid.total_cells:
                    logger.error(
                        "Stuck in boundary following loop, path may be blocked"
                    )
                    return False

            success = robot.move_to(next_pos)
            if not success:
                logger.error(f"Failed to move to {next_pos} in boundary mode")
                return False
            logger.info(f"Boundary mode - moving to {next_pos}")
            visited_in_boundary.add(next_pos)

    if robot.position == goal:
        logger.info(f"Successfully reached goal at position {goal}")
        return True
    else:
        logger.error(f"Max iterations reached without finding goal")
        return False
