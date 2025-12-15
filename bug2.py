"""
Bug2 algorithm for robot navigation with dynamic obstacle avoidance.
Implements boundary following when obstacles are encountered.
"""

from typing import List, Set, Optional, Tuple
from robot import Robot, MessageType
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class State(Enum):
    DIRECT = 1
    BOUNDARY = 2


def bug2_navigate(
    robot: Robot, goal: int, dynamic_obstacles: Optional[Set[int]] = None
) -> Tuple[bool, bool]:
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
        Tuple of (success: bool, entered_boundary_mode: bool)
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
    entered_boundary_mode = False

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
                return (False, entered_boundary_mode)

            best_neighbor, best_distance = neighbors_by_distance[0]

            # Check if we're making progress toward goal
            if best_distance < current_m_distance:
                # Good progress, move toward goal
                logger.info(f"Direct mode - moving to {best_neighbor}")
                success = robot.step(best_neighbor)
                if not success:
                    if not robot.active or robot.get_battery_level() <= 0:
                        robot.notify_aws("Battery depleted during Bug2 direct mode")
                        robot.active = False
                    logger.error(f"Failed to move to {best_neighbor} in direct mode")
                    return False
            else:
                # We've hit an obstacle. Enter boundary following mode
                logger.info(
                    f"Obstacle detected at m-distance {current_m_distance}. Entering boundary mode."
                )

                state = State.BOUNDARY
                entered_boundary_mode = True
                hit_point_distance = current_m_distance
                visited_in_boundary = {robot.position}

                # Start wall-following by picking first available neighbor
                # ? REVIEW, should we pick the best neighbor instead?
                valid_neighbors = [n for n in neighbors if not is_blocked(n)]
                if not valid_neighbors:
                    logger.error("No valid neighbors when entering boundary mode")
                    return (False, entered_boundary_mode)

                success = robot.step(valid_neighbors[0])
                if not success:
                    if not robot.active or robot.get_battery_level() <= 0:
                        robot.notify_aws("Battery depleted when entering boundary mode")
                        robot.active = False
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
                # Notify AWS that robot had to enter boundary mode and ask for re-evaluation
                robot.notify_aws(f"Bug2 boundary mode detected at position {robot.position}. Requesting task re-evaluation from current position.")
                state = State.DIRECT
                hit_point_distance = 1_000_000  # Reset to large initial value
                visited_in_boundary = set()
                continue

            # Wall-following: try to keep obstacle on right side
            # Get valid neighbors for wall-following
            valid_neighbors = [n for n in neighbors if not is_blocked(n)]

            if not valid_neighbors:
                logger.error("No valid neighbors in boundary mode, path blocked")
                return (False, entered_boundary_mode)

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
                    return (False, entered_boundary_mode)

            success = robot.step(next_pos)
            if not success:
                if not robot.active or robot.get_battery_level() <= 0:
                    robot.notify_aws("Battery depleted during Bug2 boundary mode")
                    robot.active = False
                logger.error(f"Failed to move to {next_pos} in boundary mode")
                return False
            logger.info(f"Boundary mode - moving to {next_pos}")
            visited_in_boundary.add(next_pos)

    if robot.position == goal:
        logger.info("Successfully reached goal at position %s", goal)
        return (True, entered_boundary_mode)
    else:
        logger.error(f"Max iterations reached without finding goal")
        return (False, entered_boundary_mode)
    
def execute_path(
    robot: Robot, path: List[int], dynamic_obstacles: Optional[Set[int]] = None
) -> bool:
    """
    Execute a precomputed path with dynamic obstacle detection
    If obstacle detected, switch to Bug2 algorithm

    Args:
        robot: Robot instance
        path: Precomputed path to execute
        dynamic_obstacles: Set of dynamic obstacle positions

    Returns:
        True if goal reached, False otherwise
    """
    assert path, "Path cannot be empty"
    assert (
        path[0] == robot.position
    ), f"Path must start from current position {robot.position}, got {path[0]}"

    if dynamic_obstacles is None:
        dynamic_obstacles = set()

    logger.info(f"Executing path: {path}")
    logger.info(f"Dynamic obstacles: {dynamic_obstacles}")

    goal = path[-1]

    for i in range(1, len(path)):
        next_pos = path[i]
        # Check if obstacle is a robot
        obstacle_type = get_obstacle_type(robot, next_pos)
        
        if obstacle_type == "robot":
            # Robot detected - execute collision protocol
            robot.logger.warning(f"Detected other robot at position {next_pos}!")

            other_robot = Robot.get_robot_at(next_pos)
            if other_robot is None:
                continue

            # Execute collision protocol to determine order
            am_leader = robot.collision_protocol(other_robot)

            if am_leader:
                robot.logger.info("I am leader, executing Bug2")
                # Add the other robot's position to private obstacle map
                robot.private_obstacles.add(next_pos)
                other_positions = {r.position for r in Robot._registry.values() if r.robot_id != robot.robot_id}
                success, _ = bug2_navigate(robot, goal, dynamic_obstacles.union(robot.private_obstacles).union(other_positions))
                # Send finished message with current position
                robot.logger.info(f"finished Bug2, notifying other robot at position {robot.position}")
                robot.send_message(other_robot.robot_id, MessageType.FINISHED, robot.position)
                return success
            else:
                robot.logger.info("I am follower, waiting for leader to finish")
                # Wait for leader to finish
                finished_msg = robot.receive_message(MessageType.FINISHED, timeout=10.0)
                if finished_msg is not None:
                    _, _, leader_final_pos = finished_msg
                    robot.logger.info(f"leader finished at position {leader_final_pos}, adding to private obstacles")
                    robot.private_obstacles.add(leader_final_pos)
                    other_positions = {r.position for r in Robot._registry.values() if r.robot_id != robot.robot_id}
                    return execute_path(robot, path[i - 1:], dynamic_obstacles.union(robot.private_obstacles).union(other_positions))
                else:
                    robot.logger.warning("timeout waiting for leader to finish")
                    return False
        
        # Check for static obstacles or dynamic obstacles
        if next_pos in dynamic_obstacles or robot.grid.is_shelf(next_pos):
            robot.logger.warning(f"Obstacle detected at position {next_pos}!")
            robot.logger.info("Switching to Bug2 algorithm for obstacle avoidance")

            other_positions = {r.position for r in Robot._registry.values() if r.robot_id != robot.robot_id}
            success, _ = bug2_navigate(robot, goal, dynamic_obstacles.union(other_positions))
            return success

        # Move to next position
        success = robot.step(next_pos)
        if not success:
            if not robot.active or robot.get_battery_level() <= 0:
                robot.notify_aws("Battery depleted during path execution")
                robot.active = False
            logger.error(f"Failed to move to position {next_pos}")
            return False

    logger.info(f"Successfully executed path, reached goal at {goal}")
    return True


def get_obstacle_type(robot: Robot, pos: int) -> Optional[str]:
    """Return 'robot' if a robot occupies pos, 'shelf' if shelf, else None."""
    if Robot.get_robot_at(pos) is not None:
        return "robot"
    if robot.grid.is_shelf(pos):
        return "shelf"
    return None



