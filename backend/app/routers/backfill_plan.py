"""数据回填计划接口：批量回填安排、回填合并队列、交集裁决与完成分区接收。

路由层只做参数解析和错误映射，状态流转全部在
`app.services.backfill_plan` 里完成。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.backfill_plan import BackfillPlanError, backfill_repository

router = APIRouter(prefix="/api/backfill-plans", tags=["数据回填计划"])


class BackfillRequestIn(BaseModel):
    request_id: str | None = None
    label: str | None = None
    start_partition: str | None = None
    end_partition: str | None = None
    range: str | None = None
    mode: str = "append"
    priority: int | None = None
    partition_modes: dict[str, str] = Field(default_factory=dict)
    rows: int | None = None


class BackfillPlanIn(BaseModel):
    name: str | None = None
    table: str
    requests: list[BackfillRequestIn] = Field(default_factory=list)


class ConfirmIn(BaseModel):
    token: str | None = None
    operator: str | None = None


class ResolveIn(BaseModel):
    winner_request: str
    partitions: list[str] | None = None
    operator: str | None = None
    remark: str | None = None


class CompleteIn(BaseModel):
    completed_partitions: list[str] = Field(default_factory=list)
    receipt_id: str | None = None
    source: str = "batch_merge"
    sources: dict[str, list[dict[str, Any]]] | None = None
    operator: str | None = None


class ReplayReceiptIn(BaseModel):
    receipt_id: str | None = None
    completed_partitions: list[str] = Field(default_factory=list)
    source: str = "batch_merge"


class RecoverIn(BaseModel):
    replay_receipts: list[ReplayReceiptIn] = Field(default_factory=list)


@router.get("")
def list_plans() -> dict[str, Any]:
    """回填计划清单：每个计划的覆盖范围与待处理分区。"""
    return backfill_repository.list_plans()


@router.post("")
def submit_plan(payload: BackfillPlanIn) -> dict[str, Any]:
    """一次提交多条原请求，生成可核对的分区回填计划。"""
    try:
        return backfill_repository.submit_plan(payload.model_dump())
    except BackfillPlanError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.get("/merge-queue")
def merge_queue() -> dict[str, Any]:
    """回填合并队列：交集裁决、两个原请求、非交集可提交范围。"""
    return backfill_repository.merge_queue()


@router.get("/merge-queue/{plan_id}")
def merge_queue_item(plan_id: str) -> dict[str, Any]:
    item = backfill_repository.merge_queue_item(plan_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"分区回填计划 {plan_id} 不存在")
    return item


@router.get("/{plan_id}")
def plan_detail(plan_id: str) -> dict[str, Any]:
    detail = backfill_repository.plan_detail(plan_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"分区回填计划 {plan_id} 不存在")
    return detail


@router.post("/{plan_id}/confirm")
def confirm_plan(plan_id: str, payload: ConfirmIn | None = None) -> dict[str, Any]:
    """确认计划：并发确认只落定一次，重复调用幂等。"""
    body = payload or ConfirmIn()
    result, message, _already = backfill_repository.confirm_plan(
        plan_id, token=body.token, operator=body.operator)
    if result is None:
        raise HTTPException(status_code=404, detail=message)
    ok = "仍需裁决" not in message
    return {"ok": ok, "message": message, **result}


@router.post("/{plan_id}/intersection")
def resolve_intersection(plan_id: str, payload: ResolveIn) -> dict[str, Any]:
    """交集处置：人工裁决冲突分区归属哪个原请求。"""
    result, message = backfill_repository.resolve_intersection(
        plan_id,
        winner_request=payload.winner_request,
        partitions=payload.partitions,
        operator=payload.operator,
        remark=payload.remark,
    )
    if result is None:
        status = 409 if "已确认" in message else 404 if "不存在" in message else 400
        raise HTTPException(status_code=status, detail=message)
    return {"ok": True, "message": message, **result}


@router.post("/{plan_id}/completions")
def complete_partitions(plan_id: str, payload: CompleteIn) -> dict[str, Any]:
    """批流合并入口：按计划接收实际完成的分区，逐条写入所属原请求。"""
    result, message = backfill_repository.complete_partitions(
        plan_id,
        completed_partitions=payload.completed_partitions,
        receipt_id=payload.receipt_id,
        source=payload.source,
        sources=payload.sources,
        operator=payload.operator,
    )
    if result is None:
        status = 409 if ("尚未确认" in message or "不存在" in message) else 400
        raise HTTPException(status_code=status, detail=message)
    return result


@router.post("/{plan_id}/recover")
def recover_plan(plan_id: str, payload: RecoverIn | None = None) -> dict[str, Any]:
    """断线恢复：重建回填合并队列并幂等重放回执。"""
    body = payload or RecoverIn()
    result, message = backfill_repository.recover(
        plan_id,
        replay_receipts=[item.model_dump() for item in body.replay_receipts],
    )
    if result is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, **result}
