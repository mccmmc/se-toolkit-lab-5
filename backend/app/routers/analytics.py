"""Router for analytics endpoints.
Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, case
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select
from app.database import get_session
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.models.interaction import InteractionLog

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    lab_number = lab.replace("lab-", "")
    
    # Find the lab item
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            func.lower(ItemRecord.title).contains(f"lab {lab_number}")
        )
    )
    lab_item = result.first()
    
    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    # Get all task item IDs for this lab
    result = await session.exec(
        select(ItemRecord.id).where(
            ItemRecord.parent_id == lab_item.id,
            ItemRecord.type == "task"
        )
    )
    task_ids = result.all()
    
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
    
    # Query interactions grouped by score buckets
    result = await session.exec(
        select(
            case(
                (InteractionLog.score <= 25, "0-25"),
                (InteractionLog.score <= 50, "26-50"),
                (InteractionLog.score <= 75, "51-75"),
                (InteractionLog.score <= 100, "76-100"),
            ).label("bucket"),
            func.count().label("count")
        ).where(
            InteractionLog.item_id.in_(task_ids),
            InteractionLog.score.isnot(None)
        ).group_by("bucket")
    )
    
    bucket_counts = {row.bucket: row.count for row in result.all()}
    
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
    """Per-task pass rates for a given lab."""
    lab_number = lab.replace("lab-", "")
    
    # Find the lab item
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            func.lower(ItemRecord.title).contains(f"lab {lab_number}")
        )
    )
    lab_item = result.first()
    
    if not lab_item:
        return []
    
    # Get tasks with their avg scores and attempts
    result = await session.exec(
        select(
            ItemRecord.title.label("task"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count().label("attempts")
        ).join(
            InteractionLog, ItemRecord.id == InteractionLog.item_id
        ).where(
            ItemRecord.parent_id == lab_item.id,
            ItemRecord.type == "task"
        ).group_by(ItemRecord.id, ItemRecord.title).order_by(ItemRecord.title)
    )
    
    return [
        {
            "task": row.task.strip(),
            "avg_score": round(float(row.avg_score), 1) if row.avg_score else 0.0,
            "attempts": row.attempts,
        }
        for row in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    lab_number = lab.replace("lab-", "")
    
    # Find the lab item
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            func.lower(ItemRecord.title).contains(f"lab {lab_number}")
        )
    )
    lab_item = result.first()
    
    if not lab_item:
        return []
    
    # Get all task item IDs for this lab
    result = await session.exec(
        select(ItemRecord.id).where(
            ItemRecord.parent_id == lab_item.id,
            ItemRecord.type == "task"
        )
    )
    task_ids = result.all()
    
    if not task_ids:
        return []
    
    # Group interactions by date
    result = await session.exec(
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count().label("submissions")
        ).where(
            InteractionLog.item_id.in_(task_ids)
        ).group_by("date").order_by("date")
    )
    
    return [
        {
            "date": str(row.date),
            "submissions": row.submissions,
        }
        for row in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    lab_number = lab.replace("lab-", "")
    
    # Find the lab item
    result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            func.lower(ItemRecord.title).contains(f"lab {lab_number}")
        )
    )
    lab_item = result.first()
    
    if not lab_item:
        return []
    
    # Get all task item IDs for this lab
    result = await session.exec(
        select(ItemRecord.id).where(
            ItemRecord.parent_id == lab_item.id,
            ItemRecord.type == "task"
        )
    )
    task_ids = result.all()
    
    if not task_ids:
        return []
    
    # Join interactions with learners to get student_group
    result = await session.exec(
        select(
            Learner.student_group.label("group"),
            func.avg(InteractionLog.score).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students")
        ).join(
            Learner, InteractionLog.learner_id == Learner.id
        ).where(
            InteractionLog.item_id.in_(task_ids)
        ).group_by("group").order_by("group")
    )
    
    return [
        {
            "group": row.group.strip() if row.group else "",
            "avg_score": round(float(row.avg_score), 1) if row.avg_score else 0.0,
            "students": row.students,
        }
        for row in result.all()
    ]