from grid import Grid
from robot import Robot
import dijkstra
import bug2
import logger as log_config
import warehouse_interaction_simulator as wis

log_config.configure_root_logger()
import logging

logger = logging.getLogger(__name__)

GRID_HEIGHT = 7
GRID_WIDTH = 6


def demonstrate_dijkstra(grid: Grid, start: int, goal: int):
    """Demonstrate Dijkstra's algorithm pathfinding"""
    logger.info("=" * 60)
    logger.info("DEMONSTRATING DIJKSTRA'S ALGORITHM")
    logger.info("=" * 60)

    robot = Robot(grid, start)

    logger.info("Initial grid:")
    print(grid.visualize(robot_pos=robot.position))

    result = dijkstra.find_path(robot, goal)

    if result and isinstance(result, list):
        path = result
        logger.info(f"Path found: {path}")
        logger.info(f"Path length: {len(path)} steps")
        logger.info("Grid with path:")
        print(grid.visualize(path=path, robot_pos=robot.position))
    else:
        logger.error("No path found!")


def demonstrate_dijkstra_all(grid: Grid, start: int):
    """Demonstrate Dijkstra's algorithm finding all reachable positions"""
    logger.info("=" * 60)
    logger.info("DEMONSTRATING DIJKSTRA'S ALGORITHM - ALL REACHABLE POSITIONS")
    logger.info("=" * 60)

    robot = Robot(grid, start)

    logger.info("Initial grid:")
    print(grid.visualize(robot_pos=robot.position))

    result = dijkstra.find_path(robot, goal=None)

    if not result or not isinstance(result, dict):
        logger.error("No reachable positions found!")
        return

    all_paths = result
    logger.info(f"Number of reachable positions: {len(all_paths)}")
    for destination, path in all_paths.items():
        logger.info(f"Path to {destination}: {path} (length: {len(path)} steps)")

    for destination, path in all_paths.items():
        assert path[0] == start, f"Path to {destination} does not start at {start}"
        assert (
            path[-1] == destination
        ), f"Path to {destination} does not end at {destination}"
        assert all(
            robot.grid.is_walkable(pos) for pos in path
        ), f"Path to {destination} contains non-walkable positions"


def demonstrate_bug2_success(grid: Grid, start: int, goal: int, obstacle_pos: int):
    """Demonstrate Bug2 algorithm successfully circumnavigating an obstacle"""
    logger.info("=" * 60)
    logger.info("DEMONSTRATING BUG2 - SUCCESSFUL CIRCUMNAVIGATION")
    logger.info("=" * 60)

    robot = Robot(grid, start)

    logger.info("Initial grid:")
    print(grid.visualize(robot_pos=robot.position))

    # Now place a dynamic obstacle on the path
    logger.info(f"Placing dynamic obstacle at position {obstacle_pos}")
    dynamic_obstacles = {obstacle_pos}

    # Execute path with obstacle detection
    logger.info("Robot starts executing path...")
    success = bug2.bug2_navigate(robot, goal, dynamic_obstacles)

    if success:
        logger.info(f"SUCCESS: Robot reached goal at position {goal}")
        logger.info(f"Final robot position: {robot.position}")
    else:
        logger.error("FAILED: Robot could not reach goal")
        logger.error(f"Final robot position: {robot.position}")


def demonstrate_bug2_blocked(grid: Grid, start: int, goal: int):
    """Demonstrate Bug2 algorithm failing due to blocked path"""
    logger.info("=" * 60)
    logger.info("DEMONSTRATING BUG2 - BLOCKED PATH")
    logger.info("=" * 60)

    robot = Robot(grid, start)

    logger.info("Initial grid:")
    print(grid.visualize(robot_pos=robot.position))

    # Place dynamic obstacles blocking the path
    dynamic_obstacles = {4, 10, 16, 22, 28, 34, 40}

    logger.info(f"Placing dynamic obstacles at positions: {dynamic_obstacles}")

    # Execute path with obstacle detection
    logger.info("Robot starts executing path...")
    success = bug2.bug2_navigate(robot, goal, dynamic_obstacles)

    assert not success, "Bug2 should have failed due to blocked path"
    logger.info("As expected, Bug2 navigation failed due to blocked path.")


def demonstrate_fetch_box(grid: Grid, start: int, shelf_goal: int):
    """Demonstrate fetching a box from a shelf"""
    logger.info("=" * 60)
    logger.info("DEMONSTRATING BUG2 - BLOCKED PATH")
    logger.info("=" * 60)

    robot = Robot(grid, start)

    logger.info("Initial grid:")
    print(grid.visualize(robot_pos=robot.position))

    # Get nearest adjacent position to shelf
    nearest_pos = grid.get_nearest_adjacent_walkable(robot.position, shelf_goal)
    if nearest_pos is None:
        logger.error("No reachable adjacent position to shelf!")
        return

    # Move robot to nearest position
    logger.info(f"Moving robot to position adjacent to shelf at {shelf_goal}")
    bug2.bug2_navigate(robot, nearest_pos)

    assert robot.get_position() == nearest_pos, "Robot did not reach adjacent position"
    assert robot.position in grid.get_neighbors(
        shelf_goal
    ), "Robot not adjacent to shelf"

    # Fetch item from shelf
    rfid = 12345
    success = robot.fetch_item(shelf_goal, rfid)
    if success:
        logger.info(
            f"Successfully fetched item with RFID {rfid} from shelf at {shelf_goal}"
        )
    else:
        logger.error("Failed to fetch item from shelf!")


def demonstrate_put_box(grid: Grid, start: int, cb_pos: int, shelf_goal: int):
    """Demonstrate putting a box on a shelf, taken from the CB"""
    logger.info("=" * 60)
    logger.info("DEMONSTRATING PUTTING BOX ON SHELF")
    logger.info("=" * 60)

    robot = Robot(grid, start)

    logger.info("Initial grid:")
    print(grid.visualize(robot_pos=robot.position))

    # Get nearest adjacent position to CB
    nearest_cb_pos = grid.get_nearest_adjacent_walkable(robot.position, cb_pos)
    if nearest_cb_pos is None:
        logger.error("No reachable adjacent position to CB!")
        return

    # Move robot to nearest position
    logger.info(f"Moving robot to position adjacent to CB at {cb_pos}")
    bug2.bug2_navigate(robot, nearest_cb_pos)

    assert (
        robot.get_position() == nearest_cb_pos
    ), "Robot did not reach adjacent position"
    assert robot.position in grid.get_neighbors(cb_pos), "Robot not adjacent to CB"

    # Fetch item from CB
    rfid = 67890
    success = robot.fetch_item(cb_pos, rfid)
    if success:
        logger.info(f"Successfully fetched item with RFID {rfid} from CB at {cb_pos}")
    else:
        logger.error("Failed to fetch item from CB!")
        return

    # Now move to shelf
    nearest_shelf_pos = grid.get_nearest_adjacent_walkable(robot.position, shelf_goal)
    if nearest_shelf_pos is None:
        logger.error("No reachable adjacent position to shelf!")
        return

    logger.info(f"Moving robot to position adjacent to shelf at {shelf_goal}")
    bug2.bug2_navigate(robot, nearest_shelf_pos)
    assert (
        robot.get_position() == nearest_shelf_pos
    ), f"Robot did not reach adjacent position, expected {nearest_shelf_pos}, got {robot.get_position()}"
    assert robot.position in grid.get_neighbors(
        shelf_goal
    ), "Robot not adjacent to shelf"

    # Put item on shelf
    success = robot.put_item(shelf_goal, rfid)
    if success:
        logger.info(f"Successfully put item with RFID {rfid} on shelf at {shelf_goal}")
    else:
        logger.error("Failed to put item on shelf!")
        
def demonstrate_multi_robots(grid: Grid):
        Robot._registry.clear()
        Robot._message_queue.clear()
        Robot._next_robot_id = 1

        starts = [1, 10, 20, 24, 34]
        goals = [41, 20, 10, 34, 24]

        robots = [Robot(grid, s) for s in starts]
        paths: list[list[int]] = []
        for r, g in zip(robots, goals):
            path = dijkstra.find_path(r, g)
            if not path:
                logger.error(f"No path for R{r.robot_id} to {g}")
                return
            paths.append(path)

        logger.info("Starting 5-robot scenario")

        res = wis.run_multi_robot_paths(robots, paths)
        logger.info(f"5-robot result: {res}")
        for r in robots:
            logger.info(f"Robot {r.robot_id} final pos: {r.position}")
            
def demonstrate_aws_csv(grid: Grid):
        """Send a single CSV AWS message to a robot and observe sequential execution."""
        Robot._registry.clear()
        Robot._message_queue.clear()
        Robot._next_robot_id = 1

        r1 = Robot(grid, 34)
        # Use AWS helper to build a path-based CSV (full Dijkstra paths) for the
        # scenario so the robot receives an explicit path and doesn't need to run
        # pathfinding itself.
        from aws import AWS

        aws = AWS(grid)
        demo_job = {"cb_pos": 3, "shelf_pos": 29, "rfid": 111}
        csv_msg = aws._build_csv_for_store(r1, demo_job)
        logger.info("Sending CSV AWS assignment to Robot 1: assign:0:%s", csv_msg)
        ok = r1.receive_aws_message(f"assign:0:{csv_msg}")
        logger.info("CSV processing finished (success=%s). Robot 1 final pos=%s", ok, r1.position)
    
def demonstrate_low_battery_move(grid: Grid):
        """Demonstrate that move_to refuses to start when battery is insufficient."""
        Robot._registry.clear()
        Robot._message_queue.clear()
        Robot._next_robot_id = 1

        r1 = Robot(grid, 26)

        # Register an AWS callback to capture notifications
        notifications = []

        def aws_cb(rid, message):
            notifications.append((rid, message))
            logger.info(f"AWS CALLBACK received from R{rid}: {message}")

        Robot.register_aws_callback(aws_cb)

        # Drain battery so the path to a far target cannot be completed
        r1.battery_level = 1

        # Let AWS try to assign a realistic job; AWS will check battery and either
        # assign, or create a charge job / retry.
        from aws import AWS

        aws = AWS(grid)
        job_id = aws.create_store_incoming_job(3, 29)
        logger.info("AWS created job %s; jobs state: %s", job_id, aws.jobs)
        logger.info("Notifications: %s", notifications)

def demonstrate_aws_scheduler(grid: Grid):
    """Demonstrate the AWS task scheduler assigning 'store incoming box' jobs."""
    Robot._registry.clear()
    Robot._message_queue.clear()
    Robot._next_robot_id = 1

    # Create some robots
    r1 = Robot(grid, 34)
    r2 = Robot(grid, 20)

    # Drain r1 to force AWS to reassign if needed
    r1.battery_level = 2

    from aws import AWS

    aws = AWS(grid)

    # Create a store job: CB at 3 to shelf at 8
    job_id = aws.create_store_incoming_job(3, 8)

    logger.info(f"AWS created job {job_id}; jobs state: {aws.jobs}")


def demonstrate_aws_outgoing(grid: Grid):
    """Demonstrate AWS creating an outgoing box job (shelf -> CB)."""
    Robot._registry.clear()
    Robot._message_queue.clear()
    Robot._next_robot_id = 1

    # Create robots
    r1 = Robot(grid, 34)
    r2 = Robot(grid, 20)

    from aws import AWS

    aws = AWS(grid)

    # Create an outgoing job: move a box from shelf 29 to CB 3
    job_id = aws.create_outgoing_job(29, 3)
    logger.info(f"AWS created outgoing job {job_id}; jobs state: {aws.jobs}")


def demonstrate_aws_concurrency_and_charging(grid: Grid):
    """Demonstrate AWS concurrency limit (5) and charge instructions for low-battery robots."""
    Robot._registry.clear()
    Robot._message_queue.clear()
    Robot._next_robot_id = 1

    # Create several robots (some with low battery) so AWS must decide who to assign
    robots = [Robot(grid, pos) for pos in (1, 13, 19, 25, 31)]

    aws = None
    from aws import AWS

    aws = AWS(grid)

    # Create more than `max_concurrent_assignments` jobs to exercise the limit
    created = []
    for i in range(6):
        jid = aws.create_store_incoming_job(cb_pos=3, shelf_pos=29 if i % 2 == 0 else 8)
        created.append(jid)

    logger.info("Created jobs: %s", created)

    # Print the job states after assignment attempts
    pending = [jid for jid, j in aws.jobs.items() if j.get("status") == "pending"]
    assigned = [jid for jid, j in aws.jobs.items() if j.get("status") in ("assigned", "in_progress")]
    complete = [jid for jid, j in aws.jobs.items() if j.get("status") == "complete"]

    logger.info(f"Job counts: pending={len(pending)}, assigned/in_progress={len(assigned)}, complete={len(complete)}")
    logger.info("Jobs detail: %s", aws.jobs)

    # Verify that at most max_concurrent_assignments are assigned/in_progress
    assert len(assigned) <= aws.max_concurrent_assignments

    # Show that robots with low battery have (or will receive) charge jobs
    charge_jobs = [j for j in aws.jobs.values() if j.get("type") == "charge"]
    logger.info("Charge jobs created: %s", charge_jobs)



def demonstrate_five_robots_concurrent(grid: Grid):
    from aws import AWS
    import time

    print("\n--- Demonstrating 5 robots working concurrently ---")
    # Reset robot registry if needed
    Robot._registry = {}
    Robot._next_robot_id = 1

    # Create 5 robots at different starting positions
    robots = [
        Robot(grid, start_position=21),
        Robot(grid, start_position=1),
        Robot(grid, start_position=33),
        Robot(grid, start_position=40),
        Robot(grid, start_position=26),
    ]

    aws = AWS(grid)

    # Create 5 store jobs (CB to shelf, different targets)
    jobs = []
    jobs.append(aws.create_store_incoming_job(cb_pos=6, shelf_pos=35))
    jobs.append(aws.create_store_incoming_job(cb_pos=37, shelf_pos=9))
    jobs.append(aws.create_store_incoming_job(cb_pos=6, shelf_pos=8))
    jobs.append(aws.create_store_incoming_job(cb_pos=37, shelf_pos=29))
    jobs.append(aws.create_store_incoming_job(cb_pos=6, shelf_pos=35))
    # Wait for all jobs to complete (simple polling loop)
    all_done = False
    while not all_done:
        all_done = True
        for job_id in jobs:
            status = aws.jobs[job_id]["status"]
            print(f"Job {job_id} status: {status}")
            if status not in ("complete", "failed"):
                all_done = False
        if not all_done:
            time.sleep(0.5)
    print("--- All 5 robots have completed their assignments ---\n")


def demonstrate_synchronized_aws_execution(grid: Grid):
    """Demonstrate synchronized AWS assignments with phase-based execution."""
    Robot._registry.clear()
    Robot._message_queue.clear()
    Robot._next_robot_id = 1

    # Create 5 robots at different starting positions
    robots = [
        Robot(grid, start_position=21),
        Robot(grid, start_position=1),
        Robot(grid, start_position=33),
        Robot(grid, start_position=40),
        Robot(grid, start_position=26),
    ]
    
    robots[2].battery_level = 15  # Simulate low battery for one robot

    logger.info(f"Created 5 robots at positions: {[r.position for r in robots]}")

    # Define job specs (cb_pos, shelf_pos)
    job_specs = [
        (6, 35),
        (37, 9),
        (6, 8),
        (37, 29),
        (6, 29),
    ]

    # Use warehouse_interaction_simulator to orchestrate everything
    wis.run_synchronized_aws_assignments(robots, job_specs, grid)

def main():
    """Main function demonstrating all functionality"""

    # Create grid
    grid = Grid(GRID_HEIGHT, GRID_WIDTH)

    # Add shelves at specified positions
    grid.add_shelf(8)
    grid.add_shelf(9)
    grid.add_shelf(29)
    grid.add_shelf(35)
    grid.add_cb(6)
    grid.add_cb(37)

    assert grid.is_walkable(1), "Position 1 should be walkable"
    assert not grid.is_walkable(8), "Position 8 should not be walkable"
    assert not grid.is_walkable(29), "Position 3 should not be walkable"
    assert grid.is_walkable(10), "Position 10 should be walkable"

    logger.info("Initial Grid Configuration:")
    print(grid.visualize())
    print("\nLegend: S = Shelf, C = CB, R = Robot, * = Path\n")

    # demonstrate_dijkstra(grid, start=1, goal=42)

    # demonstrate_dijkstra_all(grid, start=1)

    # demonstrate_bug2_success(grid, start=1, goal=12, obstacle_pos=6)

    # demonstrate_bug2_blocked(grid, start=1, goal=42)

    #demonstrate_fetch_box(grid, 1, 29)

    #demonstrate_put_box(grid, 1, 4, 8)

    #demonstrate_multi_robots(grid)

    #demonstrate_aws_csv(grid)
    
    #demonstrate_low_battery_move(grid)
    
    #demonstrate_aws_scheduler(grid)
    #demonstrate_aws_outgoing(grid)
    #demonstrate_aws_concurrency_and_charging(grid)
    demonstrate_synchronized_aws_execution(grid)


    logger.info("=" * 60)
    logger.info("ALL DEMONSTRATIONS COMPLETED")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
