"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, case, cast, Numeric
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter()


async def _get_lab_and_tasks(
    lab: str, session: AsyncSession
) -> tuple[ItemRecord | None, list[ItemRecord]]:
    """Find the lab by title matching and return it with its child tasks."""
    # Convert "lab-04" → "Lab 04" for title matching
    lab_title_part = lab.replace("-", " ").title().replace(" ", " ", 1)
    # Alternative: just capitalize first letter of each word part
    lab_title_part = lab.replace("-", " ").title()
    # Actually "lab-04" should match "Lab 04" in title
    lab_title_part = lab.replace("-", " ").title()
    # "lab-04" → "Lab 04"
    # But the fixture has "Lab 04 — Testing"
    # So we need to match title containing "Lab 04"
    lab_title_part = lab.replace("-", " ").title()
    # "lab-04" → "Lab 04" but title() gives "Lab 04"
    # Let's just use: capitalize first letter, keep rest
    lab_title_part = lab[0].upper() + lab[1:].replace("-", " ")
    # "lab-04" → "Lab 04"

    lab_stmt = (
        select(ItemRecord)
        .where(ItemRecord.type == "lab")
        .where(ItemRecord.title.contains(lab_title_part))
    )
    lab_result = await session.exec(lab_stmt)
    lab_item = lab_result.first()

    if not lab_item:
        return None, []

    tasks_stmt = (
        select(ItemRecord)
        .where(ItemRecord.type == "task")
        .where(ItemRecord.parent_id == lab_item.id)
        .order_by(ItemRecord.title)
    )
    tasks_result = await session.exec(tasks_stmt)
    tasks = tasks_result.all()

    return lab_item, tasks


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item by matching title (e.g. "lab-04" → title contains "Lab 04")
    - Find all tasks that belong to this lab (parent_id = lab.id)
    - Query interactions for these items that have a score
    - Group scores into buckets: "0-25", "26-50", "51-75", "76-100"
      using CASE WHEN expressions
    - Return a JSON array:
      [{"bucket": "0-25", "count": 12}, {"bucket": "26-50", "count": 8}, ...]
    - Always return all four buckets, even if count is 0
    """
    _, tasks = await _get_lab_and_tasks(lab, session)
    if not tasks:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    task_ids = [task.id for task in tasks]

    # Use CASE WHEN to bucket scores
    bucket_expr = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    )

    stmt = (
        select(bucket_expr.label("bucket"), func.count().label("count"))
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by(bucket_expr)
    )

    result = await session.exec(stmt)
    bucket_counts = dict(result.all())

    # Always return all four buckets
    return [
        {"bucket": "0-25", "count": bucket_counts.get("0-25", 0)},
        {"bucket": "26-50", "count": bucket_counts.get("26-50", 0)},
        {"bucket": "51-75", "count": bucket_counts.get("51-75", 0)},
        {"bucket": "76-100", "count": bucket_counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - For each task, compute:
      - avg_score: average of interaction scores (round to 1 decimal)
      - attempts: total number of interactions
    - Return a JSON array:
      [{"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}, ...]
    - Order by task title
    """
    _, tasks = await _get_lab_and_tasks(lab, session)
    if not tasks:
        return []

    result = []
    for task in tasks:
        stmt = (
            select(
                func.round(cast(func.avg(InteractionLog.score), Numeric), 1).label("avg_score"),
                func.count().label("attempts"),
            )
            .where(InteractionLog.item_id == task.id)
            .where(InteractionLog.score.isnot(None))
        )
        query_result = await session.exec(stmt)
        row = query_result.first()
        if row:
            avg_score, attempts = row
            result.append({
                "task": task.title,
                "avg_score": float(avg_score) if avg_score else 0.0,
                "attempts": attempts,
            })
        else:
            result.append({
                "task": task.title,
                "avg_score": 0.0,
                "attempts": 0,
            })

    return result


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Group interactions by date (use func.date(created_at))
    - Count the number of submissions per day
    - Return a JSON array:
      [{"date": "2026-02-28", "submissions": 45}, ...]
    - Order by date ascending
    """
    _, tasks = await _get_lab_and_tasks(lab, session)
    if not tasks:
        return []

    task_ids = [task.id for task in tasks]

    stmt = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count().label("submissions"),
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.date(InteractionLog.created_at))
        .order_by(func.date(InteractionLog.created_at))
    )

    result = await session.exec(stmt)
    rows = result.all()

    return [
        {"date": str(date), "submissions": count}
        for date, count in rows
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    TODO: Implement this endpoint.
    - Find the lab item and its child task items
    - Join interactions with learners to get student_group
    - For each group, compute:
      - avg_score: average score (round to 1 decimal)
      - students: count of distinct learners
    - Return a JSON array:
      [{"group": "B23-CS-01", "avg_score": 78.5, "students": 25}, ...]
    - Order by group name
    """
    _, tasks = await _get_lab_and_tasks(lab, session)
    if not tasks:
        return []

    task_ids = [task.id for task in tasks]

    stmt = (
        select(
            Learner.student_group.label("group"),
            func.round(cast(func.avg(InteractionLog.score), Numeric), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students"),
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))
        .where(InteractionLog.score.isnot(None))
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.exec(stmt)
    rows = result.all()

    return [
        {
            "group": group,
            "avg_score": float(avg_score) if avg_score else 0.0,
            "students": students,
        }
        for group, avg_score, students in rows
    ]
