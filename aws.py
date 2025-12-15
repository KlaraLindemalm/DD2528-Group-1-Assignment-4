from typing import Dict, Optional, List, Tuple
import logging
from robot import Robot

logger = logging.getLogger(__name__)


class AWS:
    def __init__(self, grid):
        self.grid = grid
        self.next_job_id = 1
        self.next_rfid = 1000
        # job_id -> job dict
        self.jobs: Dict[int, Dict] = {}
        # Maximum number of concurrent assigned/in-progress jobs
        self.max_concurrent_assignments = 5

        # Register as AWS callback for robot notifications
        Robot.register_aws_callback(self.handle_robot_notification)

    def create_store_incoming_job(self, cb_pos: int, shelf_pos: int) -> int:
        job_id = self.next_job_id
        self.next_job_id += 1
        rfid = self.next_rfid
        self.next_rfid += 1

        job = {
            "type": "store_incoming_box",
            "cb_pos": cb_pos,
            "shelf_pos": shelf_pos,
            "rfid": rfid,
            "status": "pending",
            "robot_id": None,
            "csv": None,
        }
        self.jobs[job_id] = job
        logger.info(f"Created job {job_id}: store box RFID {rfid} from {cb_pos} to {shelf_pos}")
        # Try to immediately assign
        self._try_assign_job(job_id)
        return job_id

    def create_outgoing_job(self, shelf_pos: int, cb_pos: int, rfid: Optional[int] = None) -> int:
        """Create a job to take a box from a shelf to a CB (outgoing shipment).

        The job will instruct a robot to move to a position adjacent to the shelf,
        fetch the box (using the provided or generated RFID), move to a position
        adjacent to the CB, and put the box there.
        """
        job_id = self.next_job_id
        self.next_job_id += 1

        if rfid is None:
            rfid = self.next_rfid
            self.next_rfid += 1

        job = {
            "type": "outgoing_box",
            "shelf_pos": shelf_pos,
            "cb_pos": cb_pos,
            "rfid": rfid,
            "status": "pending",
            "robot_id": None,
            "csv": None,
        }
        self.jobs[job_id] = job
        logger.info(f"Created job {job_id}: outgoing box RFID {rfid} from shelf {shelf_pos} to CB {cb_pos}")
        self._try_assign_job(job_id)
        return job_id

    def _try_assign_job(self, job_id: int) -> None:
        job = self.jobs.get(job_id)
        if not job or job["status"] != "pending":
            return

        # Respect max concurrent assignment capacity
        active = sum(1 for j in self.jobs.values() if j.get("status") in ("assigned", "in_progress"))
        if active >= self.max_concurrent_assignments:
            logger.info(f"Maximum concurrent assignments reached ({active}); job {job_id} will remain pending")
            return

        # Find available robot that is active, not busy, and can perform the task
        candidates = [r for r in Robot._registry.values() if r.active and not r.busy]
        if not candidates:
            logger.info(f"No available robots to assign job {job_id}; will retry later")
            return

        # For each candidate, build a CSV and attempt assignment.
        # The CSV contains full Dijkstra 'move' paths. The robot will evaluate battery
        for r in candidates:
            # Build a CSV appropriate for the job type
            if job["type"] == "store_incoming_box":
                csv = self._build_csv_for_store(r, job)
            elif job["type"] == "outgoing_box":
                csv = self._build_csv_for_outgoing(r, job)
            else:
                logger.warning(f"Unknown job type for job {job_id}: {job['type']}")
                csv = None
            if csv is None:
                continue

            required = r.compute_required_battery_for_csv(csv)
            if r.get_battery_level() - required <= 0:
                logger.info(f"R{r.robot_id} does not have enough battery for job {job_id} (need {required}, have {r.get_battery_level()}); instructing to charge and trying next")
                # If the robot doesn't already have a charge job, try to create one for it
                if not self._robot_has_charge_job(r.robot_id):
                    port = self._find_free_charging_port()
                    if port is not None:
                        self.create_charge_job(r.robot_id, port)
                    else:
                        logger.warning(f"No free charging ports to instruct R{r.robot_id} to charge")
                continue

            logger.info(f"Attempting to assign job {job_id} to R{r.robot_id}: {csv} (need {required})")

            # Mark assignment
            job["status"] = "assigned"
            job["robot_id"] = r.robot_id
            job["csv"] = csv

            try:
                accepted = r.receive_aws_message(f"assign:{job_id}:{csv}")
            except Exception as e:
                logger.exception("Error delivering assignment to R%s: %s", r.robot_id, e)
                accepted = False

            if accepted:
                logger.info(f"Robot R{r.robot_id} accepted job {job_id}")
                # The robot will send ACK/COMPLETE notifications via callback
                return
            else:
                logger.info(f"Robot R{r.robot_id} rejected job {job_id}; trying next candidate")
                job["status"] = "pending"
                job["robot_id"] = None
                job["csv"] = None
                continue

        # If we reach here then no candidate accepted the job. Try to get at least
        # one robot charging so the fleet can recover. Pick the robot with the
        # highest battery (if any) that doesn't already have a charge job.
        try:
            best = max(candidates, key=lambda x: x.get_battery_level())
        except ValueError:
            # candidates could be empty
            return

        if not self._robot_has_charge_job(best.robot_id):
            port = self._find_free_charging_port()
            if port is not None:
                logger.info(f"Telling R{best.robot_id} to charge at CB {port}")
                self.create_charge_job(best.robot_id, port)
            else:
                logger.warning("No free charging ports found; job will remain pending")

    def _build_csv_for_store(self, robot: Robot, job: Dict) -> Optional[str]:
        # Compute positions where the robot needs to stand to fetch and put
        cb_pos = job["cb_pos"]
        shelf_pos = job["shelf_pos"]
        pickup_stand = self.grid.get_nearest_adjacent_walkable(robot.position, cb_pos)
        if pickup_stand is None:
            logger.warning(f"No walkable adjacent to CB {cb_pos} for R{robot.robot_id}")
            return None

        drop_stand = self.grid.get_nearest_adjacent_walkable(pickup_stand, shelf_pos)
        if drop_stand is None:
            logger.warning(f"No walkable adjacent to shelf {shelf_pos} for R{robot.robot_id}")
            return None

        # Build explicit step sequences for robot: from robot.position -> pickup_stand, then pickup, then
        # pickup_stand -> drop_stand, then put.
        import dijkstra

        csv_parts: List[str] = []

        # Path to pickup stand (send the full path and trust the robot to handle it)
        path1 = dijkstra.find_path(robot.position, pickup_stand, grid=robot.grid)
        if not path1:
            logger.warning(f"No path from R{robot.robot_id} at {robot.position} to pickup stand {pickup_stand}")
            return None
        if len(path1) > 0:
            csv_parts.append("move " + " ".join(str(p) for p in path1))

        # Fetch
        csv_parts.append(f"fetch {cb_pos} {job['rfid']}")


        # Compute path from pickup_stand to drop_stand using grid directly
        path2 = dijkstra.find_path(pickup_stand, drop_stand, grid=self.grid)
        if not path2:
            logger.warning(f"No path from pickup stand {pickup_stand} to drop stand {drop_stand}")
            return None
        # move to drop stand: send the full Dijkstra path starting from pickup_stand
        if len(path2) > 0:
            csv_parts.append("move " + " ".join(str(p) for p in path2))

        # Put
        csv_parts.append(f"put {shelf_pos} {job['rfid']}")

        return ",".join(csv_parts)

    def _build_csv_for_outgoing(self, robot: Robot, job: Dict) -> Optional[str]:
        """Build CSV for an outgoing job: move to shelf, fetch, move to CB, put."""
        shelf_pos = job["shelf_pos"]
        cb_pos = job["cb_pos"]

        pickup_stand = self.grid.get_nearest_adjacent_walkable(robot.position, shelf_pos)
        if pickup_stand is None:
            logger.warning(f"No walkable adjacent to shelf {shelf_pos} for R{robot.robot_id}")
            return None

        drop_stand = self.grid.get_nearest_adjacent_walkable(pickup_stand, cb_pos)
        if drop_stand is None:
            logger.warning(f"No walkable adjacent to CB {cb_pos} for R{robot.robot_id}")
            return None

        import dijkstra

        csv_parts: List[str] = []

        # Path to shelf pickup stand
        path1 = dijkstra.find_path(robot.position, pickup_stand, grid=robot.grid)
        if not path1:
            logger.warning(f"No path from R{robot.robot_id} at {robot.position} to pickup stand {pickup_stand}")
            return None
        if len(path1) > 0:
            csv_parts.append("move " + " ".join(str(p) for p in path1))

        # Fetch from shelf
        csv_parts.append(f"fetch {shelf_pos} {job['rfid']}")

        # Path to drop stand (adjacent to CB)
        path2 = dijkstra.find_path(pickup_stand, drop_stand, grid=self.grid)
        if not path2:
            logger.warning(f"No path from pickup stand {pickup_stand} to drop stand {drop_stand}")
            return None
        if len(path2) > 0:
            csv_parts.append("move " + " ".join(str(p) for p in path2))

        # Put at CB
        csv_parts.append(f"put {cb_pos} {job['rfid']}")

        return ",".join(csv_parts)

    def _find_free_charging_port(self) -> Optional[int]:
        # A charging port is a CB position; a port is free if none of its adjacent walkable
        # positions currently have a robot standing on them.
        for cb in sorted(self.grid.cbs):
            neigh = self.grid.get_neighbors(cb)
            walkable = [p for p in neigh if self.grid.is_walkable(p)]
            # Check if any robot is on any of these positions
            occupied = any(Robot.get_robot_at(p) is not None for p in walkable)
            if not occupied and walkable:
                return cb
        return None

    def handle_robot_notification(self, robot_id: int, message: str) -> None:
        logger.info(f"AWS received notification from R{robot_id}: {message}")
        # Simple parsers for messages
        if message.startswith("ACK job"):
            # Example: 'ACK job 1' (may include extra context appended by robot notify)
            jid = self._extract_job_id_from_message(message)
            if jid is not None:
                job = self.jobs.get(jid)
                if job:
                    job["status"] = "in_progress"
                    logger.info(f"Job {jid} acknowledged by R{robot_id}")
            else:
                logger.warning("Malformed ACK message: %s", message)
            return

        if message.startswith("COMPLETE job"):
            jid = self._extract_job_id_from_message(message)
            if jid is not None:
                job = self.jobs.get(jid)
                if job:
                    job["status"] = "complete"
                    logger.info(f"Job {jid} completed by R{robot_id}")
            else:
                logger.warning("Malformed COMPLETE message: %s", message)
            return

        if "Bug2 boundary mode detected" in message:
            # Robot encountered collision and had to use Bug2 algorithm
            # Extract the robot's current position from message
            logger.info(f"R{robot_id} hit collision and recovered via Bug2, requesting task re-evaluation")
            
            # Find the job currently being executed by this robot
            current_job = None
            for jid, job in self.jobs.items():
                if job.get("robot_id") == robot_id and job.get("status") in ("assigned", "in_progress"):
                    current_job = job
                    break
            
            if current_job:
                # Check robot's remaining battery against what's needed to complete the current job
                r = Robot._registry.get(robot_id)
                if r:
                    remaining_battery = r.get_battery_level()
                    required_battery = r.compute_required_battery_for_csv(current_job["csv"])
                    
                    if remaining_battery - required_battery <= 0:
                        # Not enough battery, send robot to charge
                        logger.info(f"R{robot_id} does not have enough battery to complete job (need {required_battery}, have {remaining_battery})")
                        port = self._find_free_charging_port()
                        if port is not None:
                            logger.info(f"Instructing R{robot_id} to charge at CB {port}")
                            self.create_charge_job(robot_id, port)
                        else:
                            logger.warning("No free charging ports available for R%s", robot_id)
                    else:
                        # Has enough battery, resume task execution
                        logger.info(f"R{robot_id} has sufficient battery ({remaining_battery}) to continue job {current_job.get('robot_id')}")
            return

        if "INSUFFICIENT_BATTERY" in message or "Battery depleted" in message or message.startswith("FAILED job"):
            # Find job id in message if present
            jid = None
            parts = message.split()
            for i, p in enumerate(parts):
                if p == "job" and i + 1 < len(parts):
                    try:
                        jid = int(parts[i + 1])
                    except Exception:
                        pass

            if jid is not None and jid in self.jobs:
                job = self.jobs[jid]
                logger.info(f"Job {jid} failed/needs reassignment (notified by R{robot_id})")
                job["status"] = "pending"
                job["robot_id"] = None
                job["csv"] = None
                # Try to assign to another robot
                self._try_assign_job(jid)
                return

            # Otherwise, if robot reports low battery without job context, tell it to charge.
            # Use an assignment so `receive_aws_message` only needs to handle assignments.
            port = self._find_free_charging_port()
            if port is not None:
                logger.info(f"Instructing R{robot_id} to charge at CB {port}")
                # Create a charge job targeted at this robot
                self.create_charge_job(robot_id, port)
            else:
                logger.warning("No free charging ports available to instruct R%s", robot_id)

    def create_charge_job(self, robot_id: int, cb_pos: int) -> Optional[int]:
        """Create a charge job and assign it to a specific robot (sends assign message)."""
        job_id = self.next_job_id
        self.next_job_id += 1
        # Build a step-based CSV to move the robot to the nearest adjacent walkable to the CB
        # and then charge.
        r = Robot._registry.get(robot_id)
        if r is None:
            logger.warning("Cannot create charge job: unknown robot R%s", robot_id)
            return None

        # Find nearest adjacent walkable to cb_pos relative to robot and path to it
        import dijkstra

        nearest = self.grid.get_nearest_adjacent_walkable(r.position, cb_pos)
        if nearest is None:
            logger.warning(f"No walkable adjacent to CB {cb_pos} for R{robot_id}")
            return None

        path = dijkstra.find_path(r.position, nearest, grid=self.grid)
        if not path:
            logger.warning(f"No path from R{robot_id} at {r.position} to charging spot {nearest}")
            return None

        csv_parts = []
        # Send the full path to the robot and let it execute (consistent with other jobs)
        if len(path) > 0:
            csv_parts.append("move " + " ".join(str(p) for p in path))
        csv_parts.append(f"charge {cb_pos}")

        job = {
            "type": "charge",
            "cb_pos": cb_pos,
            "status": "assigned",
            "robot_id": robot_id,
            "csv": ",".join(csv_parts),
        }
        self.jobs[job_id] = job

        # Check battery sufficiency for the generated CSV (count move steps)
        required = 0
        for part in job["csv"].split(","):
            tok = part.strip().split()
            if not tok:
                continue
            if tok[0].lower() == "move":
                required += max(0, len(tok) - 1)
        if r.get_battery_level() - required <= 0:
            logger.info(f"R{robot_id} does not have enough battery for charging job {job_id} (need {required}, have {r.get_battery_level()})")
            job["status"] = "pending"
            return None

        try:
            r.receive_aws_message(f"assign:{job_id}:{job['csv']}")
        except Exception as e:
            logger.exception("Error delivering charge assignment to R%s: %s", robot_id, e)
            job["status"] = "pending"
            return None
        return job_id

    def _robot_has_charge_job(self, robot_id: int) -> bool:
        for j in self.jobs.values():
            if j.get("type") == "charge":
                # If job is targeted at the robot (has robot_id) and is not complete,
                # we consider that a charge job exists
                if j.get("robot_id") == robot_id and j.get("status") in ("pending", "assigned", "in_progress"):
                    return True
        return False

    def _extract_job_id_from_message(self, message: str) -> Optional[int]:
        parts = message.replace("(", " ").replace(")", " ").split()
        for i, p in enumerate(parts):
            if p == "job" and i + 1 < len(parts):
                try:
                    return int(parts[i + 1])
                except Exception:
                    return None
        return None
