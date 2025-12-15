"""
Base Robot class for grid navigation.
Provides movement capabilities with safety checks.
"""

from grid import Grid
import time
from typing import Any, Dict, List, Optional, Tuple
import logging
from enum import Enum   

logger = logging.getLogger(__name__)

"""
TODOs:
[] Battery simulation
    [X] Decrease battery on move
    [X] Recharge method
    [] Notify low battery
[X] Fetch method
[X] Put method
[X] Message receiving
"""

class MessageType(Enum):
        """Message types for robot-to-robot communication."""
        COLLISION_DETECTED = "collision_detected"
        COLLISION_ACK = "collision_ack"
        FINISHED = "finished"

class Robot:
    """Base robot class that navigates the grid with safety checks"""

    _message_queue: Dict[int, List[Tuple[int, MessageType, Any]]] = {}
    _registry: Dict[int, "Robot"] = {}
    _next_robot_id = 1

    def __init__(self, grid: Grid, start_position: int, robot_id: Optional[int] = None):
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
        # Assign robot ID
        if robot_id is None:
            robot_id = Robot._next_robot_id
            Robot._next_robot_id += 1
        self.robot_id = robot_id
        # Create a per-robot logger so messages show which robot emitted them
        self.logger = logging.getLogger(f"robot {self.robot_id}")
        
        # Initialize private obstacle map (for dynamic obstacles detected at runtime)
        self.private_obstacles: set = set()
        
        # Register robot and message queue
        Robot._registry[self.robot_id] = self
        Robot._message_queue[self.robot_id] = []
        self.active = True

        # Log via per-robot logger so the logger name column shows the robot id
        self.logger.info(f"initialized at position {start_position}")

    def step(self, new_position: int) -> bool:
        """
        Step the robot one position (adjacent) with safety checks.
        Returns True if successful, False otherwise.
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
            self.logger.warning(f"Cannot move to {new_position}: shelf detected")
            return False

        # Check private obstacle map (includes obstacles added by this robot during collision avoidance)
        if new_position in self.private_obstacles:
            self.logger.warning(f"Cannot move to {new_position}: in private obstacle map")
            return False

        if self.battery_level <= 0:
            self.logger.warning("Cannot move: battery depleted")
            self.notify_aws("Battery depleted")
            self.active = False
            return False

        if self.battery_level - 1 <= 0:
            self.logger.warning(
                f"Insufficient battery to step to {new_position} (need {required}, have {self.battery_level})"
            )
            self.notify_aws("Battery depleted during step")
            self.active = False
            return False

        old_position = self.position
        self.position = new_position
        self.battery_level -= 1

        self.logger.info(
            f"Moved {old_position} ➡ {new_position}\t[⚡️{self.battery_level}% left]"
        )
        return True

    # AWS notification mechanism: external code can register a callback
    _aws_callback = None

    @classmethod
    def register_aws_callback(cls, callback):
        cls._aws_callback = callback

    def notify_aws(self, message: str) -> None:
        """Notify AWS about an important event."""
        # Log as an error-level message and invoke callback if present
        self.logger.error(f"NOTIFY_AWS: {message}")
        if Robot._aws_callback is not None:
            try:
                Robot._aws_callback(self.robot_id, message)
            except Exception as e:
                self.logger.exception("AWS callback failed: %s", e)

    def move_to(self, target: int) -> bool:
        if not self.active:
            self.logger.warning("Cannot execute move: robot inactive due to prior error or depleted battery")
            return False

        import dijkstra
        import bug2

        if target == self.position:
            # Already there
            return True

        path = dijkstra.find_path(self, target)
        if not path:
            self.logger.warning(f"No path found to target {target}")
            return False

        required = max(0, len(path) - 1)

        if self.battery_level - required <= 0:
            self.logger.warning(
                f"Insufficient battery to move to {target} (need {required}, have {self.battery_level})"
            )
            self.notify_aws(f"Insufficient battery to move to {target} (need {required}, have {self.battery_level})")
            self.active = False
            return False

        return bug2.execute_path(self, path)

    def get_position(self) -> int:
        """Get the current position of the robot"""
        return self.position

    def recharge_battery(self):
        """Recharge the robot's battery to full"""
        self.battery_level = 100
        self.logger.info("🔋 Battery recharged to 100%")

    def get_battery_level(self) -> int:
        """Get the current battery level of the robot"""
        return self.battery_level

    def fetch_item(self, cb_pos: int, rfid: int) -> bool:
        """Fetch an item with the given RFID"""
        if (dist := self.grid.manhattan_distance(cb_pos, self.position)) != 1:
            self.logger.warning(
                f"Cannot fetch item, robot at {self.position}, not adjacent to CB at {cb_pos} (distance {dist})"
            )
        self.rfid_held = rfid
        self.logger.info(f"📦 Fetched item with RFID {rfid}")
        return True

    def put_item(self, shelf_pos: int, rfid: int) -> bool:
        """Put an item with the given RFID at the current position"""
        if (dist := self.grid.manhattan_distance(shelf_pos, self.position)) != 1:
            self.logger.warning(
                f"Cannot put item, robot at {self.position}, not adjacent to shelf at {shelf_pos} (distance {dist})"
            )
            return False
        if self.rfid_held != rfid:
            self.logger.warning(f"Cannot put item with RFID {rfid}: not held by robot")
            return False

        self.logger.info(f"📦 Put item with RFID {rfid} at position {self.position}")
        self.rfid_held = None
        return True

    def get_holding_item(self) -> bool:
        """Check if the robot is currently holding an item"""
        # Placeholder implementation
        return self.rfid_held is not None
    
    def send_message(self, target_id: int, msg_type: MessageType, payload: Any = None) -> None:
        """Send a message to another robot."""
        if target_id in Robot._message_queue:
            Robot._message_queue[target_id].append((self.robot_id, msg_type, payload))
            self.logger.info(f"sent {msg_type.value} to Robot {target_id}")

    def receive_message(self, msg_type: Optional[MessageType] = None, timeout: float = 1.0) -> Optional[Tuple[int, MessageType, Any]]:
        """
        Receive a message of a specific type, or any message if msg_type is None.
        Returns (sender_id, msg_type, payload) or None on timeout.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            queue = Robot._message_queue.get(self.robot_id, [])
            if msg_type is None:
                if queue:
                    msg = queue.pop(0)
                    self.logger.info(f"received {msg[1].value} from Robot {msg[0]}")
                    return msg
            else:
                for i, (sender_id, mtype, payload) in enumerate(queue):
                    if mtype == msg_type:
                        queue.pop(i)
                        self.logger.info(f"received {msg_type.value} from Robot {sender_id}")
                        return (sender_id, mtype, payload)
            time.sleep(0.01)
        return None

    @classmethod
    def get_robot_at(cls, position: int) -> Optional["Robot"]:
        """Get the robot at a given position, if any."""
        for robot in cls._registry.values():
            if robot.position == position:
                return robot
        return None

    def collision_protocol(self, other_robot: "Robot" | List["Robot"]) -> bool:
        """Collision ordering among one or more other robots. Return True if this
        robot is the leader (goes first). Simple protocol: exchange timestamps
        with all participants and pick min (timestamp, position, robot_id).
        """
        # Normalize to list of other robots
        others = []
        if isinstance(other_robot, list) or isinstance(other_robot, tuple):
            others = [r for r in other_robot if r.robot_id != self.robot_id]
        else:
            if other_robot.robot_id != self.robot_id:
                others = [other_robot]

        timestamp = time.time()
        payload = (timestamp, self.position, self.robot_id)

        # Broadcast COLLISION_DETECTED to all others
        for r in others:
            self.logger.info(f"sending collision message to Robot {r.robot_id}")
            self.send_message(r.robot_id, MessageType.COLLISION_DETECTED, payload)

        # Collect messages from expected participants
        expected = {r.robot_id for r in others}
        received: dict[int, tuple[float, int, int]] = {}
        deadline = time.time() + 1.0
        while time.time() < deadline and expected - set(received.keys()):
            queue = Robot._message_queue.get(self.robot_id, [])
            i = 0
            while i < len(queue):
                sender_id, mtype, pl = queue[i]
                if mtype == MessageType.COLLISION_DETECTED and sender_id in expected and sender_id not in received:
                    received[sender_id] = pl  # (timestamp, position, robot_id)
                    queue.pop(i)
                    continue
                i += 1
            time.sleep(0.01)

        # Build candidate list including self
        candidates: list[tuple[float, int, int]] = []
        candidates.append(payload)
        for sid, pl in received.items():
            if isinstance(pl, tuple) and len(pl) == 3:
                candidates.append(pl)
            else:
                t, pos = pl
                candidates.append((t, pos, sid))

        # Pick minimal by (timestamp, position, robot_id)
        leader = min(candidates, key=lambda x: (x[0], x[1], x[2]))
        is_leader = leader[2] == self.robot_id

        # Send ACKs for compatibility
        for r in others:
            self.send_message(r.robot_id, MessageType.COLLISION_ACK, None)

        if is_leader:
            self.logger.info(f"elected leader in collision (payload={payload})")
        else:
            self.logger.info(f"elected follower in collision (leader={leader[2]})")

        return is_leader

    def receive_aws_message(self, msg: str) -> bool:
        """Process a single AWS message consisting of comma-separated instructions.

        Expected CSV format (examples):
          "move 2,fetch 3 111,move 7,put 8 111"

        Supported commands:
          - move <pos>
          - charge <pos>
          - fetch <pos> [rfid]
          - put <pos> [rfid]

        The robot will execute instructions in order
        """
        if not isinstance(msg, str):
            self.logger.warning("AWS message must be a comma-separated string")
            return False

        if not self.active:
            self.logger.warning("Cannot process AWS message: robot inactive due to battery or prior error")
            return False

        overall_ok = True
        parts = [p.strip() for p in msg.split(",") if p.strip()]
        for inst in parts:
            tokens = inst.split()
            if not tokens:
                continue
            cmd = tokens[0].lower()
            try:
                match cmd:
                    case "move":
                        if len(tokens) < 2:
                            self.logger.warning("Invalid move instruction (missing target): %s", inst)
                            overall_ok = False
                            break
                        target = int(tokens[1])
                        ok = self.move_to(target)
                        if ok:
                            self.logger.info(f"AWS executed move to {target}")
                        else:
                            self.logger.warning(f"AWS move to {target} failed")
                            overall_ok = False

                    case "fetch":
                        if len(tokens) < 2:
                            self.logger.warning("Invalid fetch instruction (missing target): %s", inst)
                            overall_ok = False
                            break
                        target = int(tokens[1])
                        rfid = int(tokens[2]) if len(tokens) > 2 else -1
                        ok = self.fetch_item(target, rfid)
                        if not ok:
                            overall_ok = False

                    case "put":
                        if len(tokens) < 3:
                            self.logger.warning("Invalid put instruction (need target and rfid): %s", inst)
                            overall_ok = False
                            break
                        target = int(tokens[1])
                        rfid = int(tokens[2])
                        ok = self.put_item(target, rfid)
                        if not ok:
                            overall_ok = False
                    case "charge":
                        if len(tokens) < 2:
                            self.logger.warning("Invalid charge instruction (missing target): %s", inst)
                            overall_ok = False
                            break
                        target = int(tokens[1])
                        ok = self.charge_at(target)
                        if not ok:
                            overall_ok = False

                    case _:
                        self.logger.warning("Unknown AWS instruction: %s", inst)
                        overall_ok = False
            except Exception as e:
                self.logger.warning("Error executing AWS instruction '%s': %s", inst, e)
                overall_ok = False

        return overall_ok
