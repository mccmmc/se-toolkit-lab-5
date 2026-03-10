"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    TODO: Implement this function.
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
    current_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if current_since is not None:
                params["since"] = current_since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            if logs:
                current_since = datetime.fromisoformat(logs[-1]["submitted_at"])
            else:
                break

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    TODO: Implement this function.
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
    from sqlmodel import select
    from app.models.item import ItemRecord

    new_count = 0
    lab_short_id_to_record: dict[str, ItemRecord] = {}

    # Process labs first (items where type="lab")
    for item in items:
        if item.get("type") != "lab":
            continue

        title = item.get("title", "")
        lab_short_id = item.get("lab", "")

        # Check if lab already exists
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == title,
            )
        )
        lab_item = existing.first()

        if lab_item is None:
            # Create new lab record
            lab_item = ItemRecord(type="lab", title=title)
            session.add(lab_item)
            new_count += 1

        # Map short ID to record for later lookup
        lab_short_id_to_record[lab_short_id] = lab_item

    # Process tasks (items where type="task")
    for item in items:
        if item.get("type") != "task":
            continue

        title = item.get("title", "")
        lab_short_id = item.get("lab", "")

        # Get parent lab
        parent_lab = lab_short_id_to_record.get(lab_short_id)
        if parent_lab is None:
            # Parent lab not found, skip this task
            continue

        # Check if task already exists
        existing = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == title,
                ItemRecord.parent_id == parent_lab.id,
            )
        )
        task_item = existing.first()

        if task_item is None:
            # Create new task record
            task_item = ItemRecord(type="task", title=title, parent_id=parent_lab.id)
            session.add(task_item)
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

    Returns:
        The number of newly created interaction log entries.
    """
    from sqlmodel import select
    from app.models.learner import Learner
    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord

    new_count = 0

    # Build a lookup from (lab_short_id, task_short_id) to item title
    # For labs: key is (lab, None)
    # For tasks: key is (lab, task)
    short_id_to_title: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_short_id = item.get("lab", "")
        task_short_id = item.get("task")  # Can be None for labs
        title = item.get("title", "")
        short_id_to_title[(lab_short_id, task_short_id)] = title

    # Process each log entry
    for log in logs:
        # Step 1: Find or create Learner
        student_id = log.get("student_id", "")
        student_group = log.get("group", "")

        existing_learner = await session.exec(
            select(Learner).where(Learner.external_id == student_id)
        )
        learner = existing_learner.first()

        if learner is None:
            learner = Learner(external_id=student_id, student_group=student_group)
            session.add(learner)
            await session.flush()  # Flush to get the learner ID

        # Step 2: Find the matching item in the database
        lab_short_id = log.get("lab", "")
        task_short_id = log.get("task")  # Can be None
        item_title = short_id_to_title.get((lab_short_id, task_short_id))

        if item_title is None:
            # No matching item found, skip this log
            continue

        existing_item = await session.exec(
            select(ItemRecord).where(ItemRecord.title == item_title)
        )
        item = existing_item.first()

        if item is None:
            # Item not found in DB, skip this log
            continue

        # Step 3: Check if InteractionLog with this external_id already exists
        log_external_id = log.get("id")
        existing_interaction = await session.exec(
            select(InteractionLog).where(InteractionLog.external_id == log_external_id)
        )
        if existing_interaction.first() is not None:
            # Already exists, skip for idempotent upsert
            continue

        # Step 4: Create new InteractionLog
        interaction = InteractionLog(
            external_id=log_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=datetime.fromisoformat(log.get("submitted_at")),
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    Returns:
        A dict with:
        - new_records: Number of newly created interaction logs
        - total_records: Total number of interaction logs in the database
    """
    from sqlmodel import select, func
    from app.models.interaction import InteractionLog

    # Step 1: Fetch items from the API and load them into the database
    raw_items = await fetch_items()
    await load_items(raw_items, session)

    # Step 2: Determine the last synced timestamp
    # Query the most recent created_at from InteractionLog
    latest_log = await session.exec(
        select(InteractionLog)
        .order_by(InteractionLog.created_at.desc())
        .limit(1)
    )
    last_record = latest_log.first()
    since = last_record.created_at if last_record else None

    # Step 3: Fetch logs since that timestamp and load them
    # Pass the raw_items list to load_logs so it can map short IDs to titles
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, raw_items, session)

    # Get total count of interaction logs in the database
    total_count = await session.exec(select(func.count(InteractionLog.id)))

    return {
        "new_records": new_records,
        "total_records": total_count.one(),
    }
