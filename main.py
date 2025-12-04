from grid import Grid
from robot import Robot
import dijkstra
import bug2
import logger as log_config

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
    all_dijkstras = dijkstra.find_path(robot, goal=None)
    if not all_dijkstras or not isinstance(all_dijkstras, dict):
        logger.error("No path found to shelf!")
        return
    shelf_neighbors = grid.get_neighbors(shelf_goal)
    nearest_pos = min(
        shelf_neighbors,
        key=lambda pos: (
            len(all_dijkstras.get(pos, [])) if pos in all_dijkstras else float("inf")
        ),
    )
    if nearest_pos is float("inf"):
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


def main():
    """Main function demonstrating all functionality"""

    # Create grid
    grid = Grid(GRID_HEIGHT, GRID_WIDTH)

    # Add shelves at specified positions
    grid.add_shelf(8)
    grid.add_shelf(9)
    grid.add_shelf(29)
    grid.add_shelf(35)
    grid.add_cb(3)
    grid.add_cb(4)

    logger.info("Initial Grid Configuration:")
    print(grid.visualize())
    print("\nLegend: X = Shelf, R = Robot, * = Path\n")

    # Test Manhattan distance
    logger.info("Testing Manhattan Distance:")
    test_distances = [
        (1, 7, 1),
        (1, 2, 1),
        (1, 8, 2),
        (1, 42, 11),  # Position 1 at (0,0), position 42 at (6,5): |0-6| + |0-5| = 11
    ]

    for pos1, pos2, expected in test_distances:
        distance = grid.manhattan_distance(pos1, pos2)
        logger.info(
            f"Distance from {pos1} to {pos2}: {distance} (expected: {expected})"
        )
        assert (
            distance == expected
        ), f"Distance mismatch: got {distance}, expected {expected}"

    # demonstrate_dijkstra(grid, start=1, goal=42)

    # demonstrate_dijkstra_all(grid, start=1)

    # demonstrate_bug2_success(grid, start=1, goal=12, obstacle_pos=6)

    # demonstrate_bug2_blocked(grid, start=1, goal=42)

    demonstrate_fetch_box(grid, 1, 29)

    logger.info("=" * 60)
    logger.info("ALL DEMONSTRATIONS COMPLETED")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
