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
        logger.info("Successfully reached goal at position %s", goal)
        return True
    else:
        logger.error(f"Max iterations reached without finding goal")
        return False
    
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
                # Run Bug2 to circumnavigate
                success = bug2_navigate(robot, goal, dynamic_obstacles.union(robot.private_obstacles))
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
                    # Continue executing path from current position, avoiding leader's position
                    return execute_path(robot, path[i - 1:], dynamic_obstacles.union(robot.private_obstacles))
                else:
                    robot.logger.warning("timeout waiting for leader to finish")
                    return False
        
        # Check for static obstacles or dynamic obstacles
        if next_pos in dynamic_obstacles or robot.grid.is_shelf(next_pos):
            robot.logger.warning(f"Obstacle detected at position {next_pos}!")
            robot.logger.info("Switching to Bug2 algorithm for obstacle avoidance")

            # Switch to Bug2 to circumnavigate
            return bug2_navigate(robot, goal, dynamic_obstacles)

        # Move to next position
        success = robot.move_to(next_pos)
        assert success, f"Failed to move to position {next_pos}"

    logger.info(f"Successfully executed path, reached goal at {goal}")
    return True


def get_obstacle_type(robot: Robot, pos: int) -> Optional[str]:
    """Return 'robot' if a robot occupies pos, 'shelf' if shelf, else None."""
    if Robot.get_robot_at(pos) is not None:
        return "robot"
    if robot.grid.is_shelf(pos):
        return "shelf"
    return None


def handle_pair_collision(
    robot1: Robot,
    goal1: int,
    robot2: Robot,
    goal2: int,
    dynamic_obstacles: Optional[Set[int]] = None,
    timeout: float = 10.0,
) -> dict:
    """
    Handle a collision between two robots: run the collision ordering protocol,
    let the leader execute Bug2 to its goal while the follower waits, then
    notify the follower of the leader's final position so it can update its
    private obstacles and resume.

    Returns a dict with keys: 'success1', 'success2', 'leader1', 'leader2'
    """
    if dynamic_obstacles is None:
        dynamic_obstacles = set()

    logger.info(f"Handling pair collision between Robot {robot1.robot_id} and Robot {robot2.robot_id}")

    # Each robot runs the collision_protocol (they will exchange messages)
    leader1 = robot1.collision_protocol(robot2)
    leader2 = robot2.collision_protocol(robot1)

    success1 = True
    success2 = True

    # Leader actions for robot1
    if leader1:
        logger.info(f"Robot {robot1.robot_id} is leader: navigating to {goal1}")
        robot1.private_obstacles.add(robot2.position)
        success1 = bug2_navigate(robot1, goal1, dynamic_obstacles.union(robot1.private_obstacles))
        robot1.send_message(robot2.robot_id, MessageType.FINISHED, robot1.position)
    else:
        logger.info(f"Robot {robot1.robot_id} is follower: waiting for leader to finish")
        msg = robot1.receive_message(MessageType.FINISHED, timeout=timeout)
        if msg is not None:
            _, _, leader_pos = msg
            robot1.private_obstacles.add(leader_pos)
        else:
            logger.warning(f"Robot {robot1.robot_id}: timeout waiting for leader FINISHED")
            success1 = False

    # Leader actions for robot2
    if leader2:
        logger.info(f"Robot {robot2.robot_id} is leader: navigating to {goal2}")
        robot2.private_obstacles.add(robot1.position)
        success2 = bug2_navigate(robot2, goal2, dynamic_obstacles.union(robot2.private_obstacles))
        robot2.send_message(robot1.robot_id, MessageType.FINISHED, robot2.position)
    else:
        logger.info(f"Robot {robot2.robot_id} is follower: waiting for leader to finish")
        msg = robot2.receive_message(MessageType.FINISHED, timeout=timeout)
        if msg is not None:
            _, _, leader_pos = msg
            robot2.private_obstacles.add(leader_pos)
        else:
            logger.warning(f"Robot {robot2.robot_id}: timeout waiting for leader FINISHED")
            success2 = False

    return {
        "success1": success1,
        "success2": success2,
        "leader1": leader1,
        "leader2": leader2,
    }


def _pre_move_collision(
    robot1: Robot, idx1: int, path1: List[int], robot2: Robot, idx2: int, path2: List[int]
) -> tuple[bool, Optional[int], Optional[int]]:
    """Check pre-move collision between two robots.

    Returns (collision_detected, next1, next2). If no collision, next1/next2 are None.
    """
    if idx1 < len(path1) and idx2 < len(path2):
        next1 = path1[idx1]
        next2 = path2[idx2]
        if (
            next1 == next2
            or next1 == robot2.position
            or next2 == robot1.position
            or (robot1.position == next2 and robot2.position == next1)
        ):
            return True, next1, next2
    return False, None, None


def _step_robot(robot: Robot, path: List[int], idx: int) -> tuple[int, bool]:
    """Attempt a single step for `robot` along `path` at index `idx`.

    Returns a tuple (new_idx, success_flag). If `new_idx == len(path)` the robot
    has finished or stopped due to obstacle/failure.
    """
    if idx >= len(path):
        return idx, True

    next_pos = path[idx]
    if next_pos not in robot.private_obstacles and robot.grid.is_walkable(next_pos):
        moved = robot.move_to(next_pos)
        if moved:
            return idx + 1, True
        else:
            return len(path), False
    else:
        return len(path), False


def run_multi_robot_paths(
    robots: List[Robot],
    paths: List[List[int]],
    dynamic_obstacles: Optional[Set[int]] = None,
    timeout: float = 10.0,
) -> dict:
    """Run multiple robots concurrently along precomputed paths.

    robots: list of Robot instances
    paths: list of corresponding paths (each a list of positions)
    """
    if dynamic_obstacles is None:
        dynamic_obstacles = set()

    assert len(robots) == len(paths), "Robots and paths length mismatch"
    n = len(robots)

    logger.info(f"Running concurrent paths for robots: {[r.robot_id for r in robots]}")

    # Indices start at 1 because path[0] is the starting position
    idxs = [1] * n
    success = [True] * n

    while any(idxs[i] < len(paths[i]) for i in range(n)):
        # Build next positions for those who have a next step
        next_pos = {}
        for i in range(n):
            if idxs[i] < len(paths[i]):
                next_pos[i] = paths[i][idxs[i]]

        # Build collision graph (undirected) among robots wishing to move
        graph: dict[int, set[int]] = {i: set() for i in range(n)}
        for i in next_pos:
            for j in next_pos:
                if i >= j:
                    continue
                ni = next_pos[i]
                nj = next_pos[j]
                pi = robots[i].position
                pj = robots[j].position
                if ni == nj or ni == pj or nj == pi or (pi == nj and pj == ni):
                    graph[i].add(j)
                    graph[j].add(i)

        # Find connected components of collisions
        visited = set()
        groups: List[List[int]] = []
        for i in range(n):
            if i in visited or not graph[i]:
                continue
            stack = [i]
            comp = []
            while stack:
                v = stack.pop()
                if v in visited:
                    continue
                visited.add(v)
                comp.append(v)
                for nb in graph[v]:
                    if nb not in visited:
                        stack.append(nb)
            if len(comp) > 1:
                groups.append(comp)

        # Resolve each collision group
        if groups:
            for comp in groups:
                group_robots = [robots[i] for i in comp]

                # Each robot runs collision protocol against the whole group
                leaders = []
                for r in group_robots:
                    if r.collision_protocol(group_robots):
                        leaders.append(r)

                # Pick deterministic leader if multiple
                if not leaders:
                    leader = min(group_robots, key=lambda r: r.robot_id)
                else:
                    leader = min(leaders, key=lambda r: r.robot_id)

                leader_idx = robots.index(leader)
                leader_goal = paths[leader_idx][-1]

                # Leader navigates around; followers wait
                leader.logger.info(f"Leader in group {comp} is R{leader.robot_id}")
                for i in comp:
                    if i != leader_idx:
                        leader.private_obstacles.add(robots[i].position)

                leader_success = bug2_navigate(leader, leader_goal, dynamic_obstacles.union(leader.private_obstacles) if dynamic_obstacles else leader.private_obstacles)

                # Notify followers
                for i in comp:
                    if i == leader_idx:
                        continue
                    leader.send_message(robots[i].robot_id, MessageType.FINISHED, leader.position)

                # Followers update private obstacles
                for i in comp:
                    if i == leader_idx:
                        success[i] = leader_success
                        idxs[i] = len(paths[i])
                        continue
                    msg = robots[i].receive_message(MessageType.FINISHED, timeout=timeout)
                    if msg is not None:
                        _, _, leader_pos = msg
                        robots[i].private_obstacles.add(leader_pos)
                    else:
                        robots[i].logger.warning("timeout waiting for group leader FINISHED")
                        success[i] = False

            # After handling groups, continue to next loop iteration
            continue

        # No group collisions; step all robots one by one
        for i in range(n):
            if idxs[i] < len(paths[i]):
                idxs[i], ok = _step_robot(robots[i], paths[i], idxs[i])
                if not ok:
                    success[i] = False
                    idxs[i] = len(paths[i])

    # Build results
    results = {f"success{i+1}": success[i] for i in range(n)}
    for i in range(n):
        results[f"final{i+1}"] = robots[i].position
    return results


def run_two_robot_paths(
    robots: List[Robot],
    paths: List[List[int]],
    dynamic_obstacles: Optional[Set[int]] = None,
    timeout: float = 10.0,
) -> dict:
    """Deprecated alias for run_multi_robot_paths."""
    logger.warning("run_two_robot_paths is deprecated, use run_multi_robot_paths")
    return run_multi_robot_paths(robots, paths, dynamic_obstacles, timeout)

