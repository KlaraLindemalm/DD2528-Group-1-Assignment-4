from typing import List, Set, Optional, Tuple, Dict
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


def run_synchronized_aws_assignments(
    robots: List[Robot],
    job_specs: List[Tuple[int, int]],
    grid,
) -> Dict:
    """Orchestrate AWS assignments and synchronized phase-based execution.

    robots: list of Robot instances
    job_specs: list of (cb_pos, shelf_pos) tuples for store jobs
    grid: Grid instance

    Phases:
    1. AWS creates and assigns all jobs (builds full-path CSVs)
    2. All robots execute 'move' instructions synchronously
    3. All robots execute 'fetch' instructions synchronously
    4. All robots execute 'move' instructions again
    5. All robots execute 'put' instructions synchronously
    """
    from aws import AWS

    logger.info("=" * 60)
    logger.info("SYNCHRONIZED AWS ASSIGNMENTS & PHASE-BASED EXECUTION")
    logger.info("=" * 60)

    aws = AWS(grid)

    # Phase 1: AWS creates all jobs (without assigning them yet)
    logger.info("\n--- PHASE 1: AWS creates all jobs ---")
    job_ids = []
    
    # Create all jobs in pending state first
    # We do this by creating Job objects directly instead of calling create_store_incoming_job()
    # which would immediately try to assign them
    for i, (cb_pos, shelf_pos) in enumerate(job_specs):
        job_id = aws.next_job_id
        aws.next_job_id += 1
        job = {
            "id": job_id,
            "type": "store_incoming_box",
            "status": "pending",
            "cb_pos": cb_pos,
            "shelf_pos": shelf_pos,
            "rfid": 1000 + i,  # Generate unique RFID for each job
            "robot_id": None,
            "csv": None,
        }
        aws.jobs[job_id] = job
        job_ids.append(job_id)
        logger.info(f"Created job {job_id}: CB {cb_pos} -> shelf {shelf_pos}")

    # Phase 2: Distribute assignments fairly (round-robin) across all robots
    logger.info("\n--- PHASE 2: AWS assigns jobs to robots (round-robin) ---")
    robot_list = list(robots)
    robot_idx = 0
    robot_assignments: Dict[int, List[Tuple[int, str]]] = {}
    
    for jid in job_ids:
        job = aws.jobs[jid]
        
        # Find next robot that has sufficient battery and isn't out of robots
        attempts = 0
        while attempts < len(robot_list):
            robot = robot_list[robot_idx % len(robot_list)]
            robot_idx += 1
            attempts += 1
            
            # Check if robot is active
            if not robot.active:
                continue
            
            # Build CSV for this job
            if job["type"] == "store_incoming_box":
                csv = aws._build_csv_for_store(robot, job)
            else:
                csv = None
            
            if not csv:
                logger.warning(f"Could not build CSV for job {jid} on R{robot.robot_id}")
                continue
            
            # Check battery
            required = robot.compute_required_battery_for_csv(csv)
            if robot.get_battery_level() - required <= 0:
                logger.info(f"R{robot.robot_id} lacks battery for job {jid} (need {required}, have {robot.get_battery_level()})")
                continue
            
            # Assign to this robot
            job["status"] = "assigned"
            job["robot_id"] = robot.robot_id
            job["csv"] = csv
            
            if robot.robot_id not in robot_assignments:
                robot_assignments[robot.robot_id] = []
            robot_assignments[robot.robot_id].append((jid, csv))
            
            logger.info(f"Assigned job {jid} to R{robot.robot_id}")
            break
        
        if not job["robot_id"]:
            logger.warning(f"Could not assign job {jid} to any robot")

    logger.info(f"Assignments created: {len(robot_assignments)} robots have jobs")
    for rid, assignments in robot_assignments.items():
        logger.info(f"  R{rid}: {len(assignments)} job(s)")

    # Phase 3+: Execute all robots' jobs
    # Organize jobs by robot with full instruction sequences
    logger.info("\n--- PHASE 3+: Execute movements simultaneously, fetch/put sequentially ---")
    
    # Build per-robot job queue: robot_id -> [(job_id, [instructions]), ...]
    robot_jobs: Dict[int, List[Tuple[int, List[Tuple[str, List[str]]]]]] = {}
    for robot_id, assignments in robot_assignments.items():
        robot_jobs[robot_id] = []
        for job_id, csv in assignments:
            instructions = []
            for inst_str in csv.split(","):
                inst_str = inst_str.strip()
                tokens = inst_str.split()
                if tokens:
                    cmd = tokens[0].lower()
                    instructions.append((cmd, tokens))
            robot_jobs[robot_id].append((job_id, instructions))

    total_instructions = sum(sum(len(inst) for _, inst in jobs) for jobs in robot_jobs.values())
    logger.info(f"Total instructions across all robots: {total_instructions}")

    # Strategy: Execute in phases - all moves simultaneously, then all fetches, then all moves, then all puts
    # This ensures robots move together while respecting fetch/put dependencies
    
    # Extract all instructions by type and track state
    phase = 4
    instruction_pointers: Dict[int, Dict[int, int]] = {}  # robot_id -> job_id -> instruction_index
    
    # Initialize pointers
    for robot_id, jobs in robot_jobs.items():
        instruction_pointers[robot_id] = {}
        for job_id, _ in jobs:
            instruction_pointers[robot_id][job_id] = 0
    
    # Keep executing until all instructions are done
    while True:
        # Collect all move instructions at current position
        moves_to_execute: Dict[int, List[Tuple[int, Tuple[str, List[str]]]]] = {}  # robot_id -> [(job_id, instruction), ...]
        
        for robot_id, jobs in robot_jobs.items():
            moves_to_execute[robot_id] = []
            for job_id, instructions in jobs:
                idx = instruction_pointers[robot_id][job_id]
                if idx < len(instructions):
                    cmd, tokens = instructions[idx]
                    if cmd == "move":
                        moves_to_execute[robot_id].append((job_id, (cmd, tokens)))
        
        # If there are any moves, execute them all simultaneously using run_multi_robot_paths
        if any(moves_to_execute.values()):
            logger.info(f"\n--- PHASE {phase}: All robots move simultaneously ---")
            phase += 1
            
            # Log all moves
            for robot_id, moves in moves_to_execute.items():
                for job_id, (cmd, tokens) in moves:
                    logger.info(f"  R{robot_id} (job {job_id}): move {' '.join(tokens[1:])}")
            
            # Build paths for run_multi_robot_paths
            robots_moving = []
            paths_moving = []
            robot_to_job = {}  # For tracking which job each robot is executing
            
            for robot_id, moves in moves_to_execute.items():
                robot = Robot._registry.get(robot_id)
                if not robot or not moves:
                    continue
                
                # Each robot should have only one move at a time
                job_id, (cmd, tokens) = moves[0]
                if len(tokens) > 1:
                    path = [int(p) for p in tokens[1:]]
                    # Normalize path: ensure it starts at current position
                    if path and path[0] != robot.position:
                        if len(path) > 1 and path[1] == robot.position:
                            path = path[1:]
                        elif robot.position not in path:
                            # Recompute path if inconsistent
                            import dijkstra
                            recomputed = dijkstra.find_path(robot.position, path[-1], grid=grid)
                            if recomputed:
                                path = recomputed
                    
                    robots_moving.append(robot)
                    paths_moving.append(path)
                    robot_to_job[robot.robot_id] = (job_id, robot_id)
            
            # Execute all moves simultaneously
            if robots_moving:
                run_multi_robot_paths(robots_moving, paths_moving)
            
            # Advance pointers for all robots that moved
            for robot_id, moves in moves_to_execute.items():
                if moves:
                    job_id, _ = moves[0]
                    instruction_pointers[robot_id][job_id] += 1
        
        # Collect all fetch/put/charge instructions at current position
        other_instructions: Dict[int, List[Tuple[int, Tuple[str, List[str]]]]] = {}  # robot_id -> [(job_id, instruction), ...]
        
        for robot_id, jobs in robot_jobs.items():
            other_instructions[robot_id] = []
            for job_id, instructions in jobs:
                idx = instruction_pointers[robot_id][job_id]
                if idx < len(instructions):
                    cmd, tokens = instructions[idx]
                    if cmd in ("fetch", "put", "charge"):
                        other_instructions[robot_id].append((job_id, (cmd, tokens)))
        
        # If there are any fetch/put/charge, execute them per-robot sequentially
        if any(other_instructions.values()):
            logger.info(f"\n--- PHASE {phase}: Robots execute fetch/put/charge sequentially ---")
            phase += 1
            
            for robot_id, instructions in other_instructions.items():
                robot = Robot._registry.get(robot_id)
                if not robot or not instructions:
                    continue
                
                for job_id, (cmd, tokens) in instructions:
                    logger.info(f"  R{robot_id} (job {job_id}): {cmd} {' '.join(tokens[1:])}")
                    
                    if cmd == "fetch":
                        if len(tokens) >= 3:
                            target = int(tokens[1])
                            rfid = int(tokens[2])
                            robot.fetch_item(target, rfid)
                    
                    elif cmd == "put":
                        if len(tokens) >= 3:
                            target = int(tokens[1])
                            rfid = int(tokens[2])
                            robot.put_item(target, rfid)
                    
                    elif cmd == "charge":
                        if len(tokens) >= 2:
                            cb_pos = int(tokens[1])
                            robot.charge_at(cb_pos)
                    
                    # Advance pointer for this job
                    instruction_pointers[robot_id][job_id] += 1
        
        # Check if all instructions have been executed
        all_done = all(
            instruction_pointers[robot_id][job_id] >= len(instructions)
            for robot_id, jobs in robot_jobs.items()
            for job_id, instructions in jobs
        )
        if all_done:
            break

    logger.info("\n--- All robots have completed all instructions in synchronized phases ---")
    logger.info("Final positions:")
    for robot in robots:
        logger.info(f"R{robot.robot_id}: position={robot.position}, battery={robot.battery_level}%")

    return {"completed": True}
