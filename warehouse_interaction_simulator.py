from typing import List, Set, Optional, Tuple
import logging
from robot import Robot, MessageType
import bug2

logger = logging.getLogger(__name__)


def _step_robot(robot: Robot, path: List[int], idx: int) -> tuple[int, bool]:
    """Attempt a single step for `robot` along `path` at index `idx`.

    Returns a tuple (new_idx, success_flag). If `new_idx == len(path)` the robot
    has finished or stopped due to obstacle/failure.
    """
    if idx >= len(path):
        return idx, True

    next_pos = path[idx]
    if next_pos not in robot.private_obstacles and robot.grid.is_walkable(next_pos):
        moved = robot.step(next_pos)
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

                other_positions = {r.position for r in Robot._registry.values() if r.robot_id != leader.robot_id}
                leader_dynamic = dynamic_obstacles.union(leader.private_obstacles).union(other_positions) if dynamic_obstacles else leader.private_obstacles.union(other_positions)
                leader_success = bug2.bug2_navigate(leader, leader_goal, leader_dynamic)

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
