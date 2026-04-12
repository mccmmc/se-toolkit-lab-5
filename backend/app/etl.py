"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    Uses httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    with HTTP Basic Auth (email + password from settings).
    The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    Returns the parsed list of dicts.
    Raises an exception if the response status is not 200.
    """
    timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=httpx.BasicAuth(
                settings.autochecker_email, settings.autochecker_password
            ),
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    all_logs: list[dict] = []
    timeout = httpx.Timeout(connect=10.0, read=300.0, write=10.0, pool=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            params: dict[str, str | int] = {"limit": 50}
            if since is not None:
                params["since"] = since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                params=params,
                auth=httpx.BasicAuth(
                    settings.autochecker_email, settings.autochecker_password
                ),
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            # Use the last log's submitted_at as the new since value
            if logs:
                since = datetime.fromisoformat(logs[-1]["submitted_at"])

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    from app.models.item import ItemRecord

    new_count = 0
    lab_id_to_record: dict[str, ItemRecord] = {}

    # Process labs first
    labs = [item for item in items if item["type"] == "lab"]
    for lab in labs:
        # Check if lab already exists
        statement = (
            select(ItemRecord)
            .where(ItemRecord.type == "lab")
            .where(ItemRecord.title == lab["title"])
        )
        existing_result = await session.exec(statement)
        existing_lab = existing_result.first()

        if existing_lab:
            lab_id_to_record[lab["lab"]] = existing_lab
        else:
            # Create new lab
            new_lab = ItemRecord(type="lab", title=lab["title"])
            session.add(new_lab)
            await session.flush()  # Get the ID
            lab_id_to_record[lab["lab"]] = new_lab
            new_count += 1

    # Process tasks
    tasks = [item for item in items if item["type"] == "task"]
    for task in tasks:
        parent_lab = lab_id_to_record.get(task["lab"])
        if not parent_lab:
            # Skip tasks whose parent lab wasn't found
            continue

        # Check if task already exists with this title and parent_id
        statement = (
            select(ItemRecord)
            .where(ItemRecord.type == "task")
            .where(ItemRecord.title == task["title"])
            .where(ItemRecord.parent_id == parent_lab.id)
        )
        existing_result = await session.exec(statement)
        existing_task = existing_result.first()

        if not existing_task:
            # Create new task
            new_task = ItemRecord(
                type="task", title=task["title"], parent_id=parent_lab.id
            )
            session.add(new_task)
            await session.flush()
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner

    # Build lookup from (lab_short_id, task_short_id) to title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        if item["type"] == "lab":
            item_title_lookup[(item["lab"], None)] = item["title"]
        elif item["type"] == "task":
            item_title_lookup[(item["lab"], item["task"])] = item["title"]

    new_count = 0
    for log in logs:
        # 1. Find or create Learner
        statement = select(Learner).where(Learner.external_id == log["student_id"])
        learner_result = await session.exec(statement)
        learner = learner_result.first()

        if not learner:
            learner = Learner(
                external_id=log["student_id"], student_group=log["group"]
            )
            session.add(learner)
            await session.flush()

        # 2. Find the matching item
        item_title = item_title_lookup.get((log["lab"], log.get("task")))
        if not item_title:
            # Skip if no matching item found
            continue

        item_result = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = item_result.first()

        if not item:
            # Skip if item not in database
            continue

        # 3. Check if InteractionLog already exists (idempotency)
        statement = select(InteractionLog).where(
            InteractionLog.external_id == log["id"]
        )
        existing_log_result = await session.exec(statement)
        if existing_log_result.first():
            continue

        # 4. Create InteractionLog
        created_at = datetime.fromisoformat(log["submitted_at"])
        new_interaction = InteractionLog(
            external_id=log["id"],
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=created_at,
        )
        session.add(new_interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    from app.models.interaction import InteractionLog

    # Step 1: Fetch and load items
    raw_items = await fetch_items()
    await load_items(raw_items, session)

    # Step 2: Determine the last synced timestamp
    statement = (
        select(InteractionLog)
        .order_by(InteractionLog.created_at.desc())
        .limit(1)
    )
    most_recent = await session.exec(statement)
    most_recent_log = most_recent.first()
    since = most_recent_log.created_at if most_recent_log else None

    # Step 3: Fetch and load logs
    raw_logs = await fetch_logs(since=since)
    new_records = await load_logs(raw_logs, raw_items, session)

    # Get total count
    total_result = await session.exec(select(InteractionLog))
    total_records = len(total_result.all())

    return {"new_records": new_records, "total_records": total_records}
