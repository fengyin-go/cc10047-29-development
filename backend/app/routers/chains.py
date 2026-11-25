"""跨模块业务链路：把多个领域服务的 domain_xxx 方法串联为端到端业务流程。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import service_for

router = APIRouter(prefix="/api/chains", tags=["业务链路"])

# ------------------------------------------------------------------
# 链路1: 实时流入
# stream_source→stream_ingest→commit_version→delta_ingest
# ------------------------------------------------------------------

@router.post("/stream-ingest")
def chain_stream_ingest(source_id: int, ingest_id: int, commit_id: int) -> dict:
    """实时流入链路：探测源Schema → 批次摄入 → 提交版本 → 增量摄入。"""
    steps = []

    # Step 1: 探测源Schema
    svc_src = service_for("stream_source")
    if not svc_src:
        raise HTTPException(404, "stream_source 服务未注册")
    r1 = svc_src.domain_detect_schema(source_id)
    steps.append({"step": "detect_schema", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "stream-ingest", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 批次摄入
    svc_ingest = service_for("stream_ingest")
    if not svc_ingest:
        raise HTTPException(404, "stream_ingest 服务未注册")
    r2 = svc_ingest.domain_ingest_batch(ingest_id, batch_size=1000)
    steps.append({"step": "ingest_batch", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "stream-ingest", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 提交版本
    svc_commit = service_for("delta_commit")
    if not svc_commit:
        raise HTTPException(404, "delta_commit 服务未注册")
    r3 = svc_commit.domain_commit(commit_id)
    steps.append({"step": "commit_version", "ok": r3["ok"], "details": r3})
    if not r3["ok"]:
        return {"chain": "stream-ingest", "ok": False, "steps": steps, "error": r3.get("message")}

    # Step 4: 增量摄入
    r4 = svc_commit.domain_ingest_delta(commit_id, delta_records=500)
    steps.append({"step": "delta_ingest", "ok": r4["ok"], "details": r4})

    return {"chain": "stream-ingest", "ok": True, "steps": steps, "summary": {
        "ingested_rows": r2.get("ingested_rows"),
        "committed_version": r3.get("version"),
        "delta_records": r4.get("delta_records"),
    }}


# ------------------------------------------------------------------
# 链路2: 存储优化
# lake_table→partition_merge→compaction_job→table_compaction
# ------------------------------------------------------------------

@router.post("/storage-optimize")
def chain_storage_optimize(table_id: int, merge_id: int, compaction_id: int) -> dict:
    """存储优化链路：定义湖表 → 合并分区 → 规划压缩 → 压缩表。"""
    steps = []

    # Step 1: 定义湖表
    svc_table = service_for("lake_table")
    if not svc_table:
        raise HTTPException(404, "lake_table 服务未注册")
    r1 = svc_table.domain_define_table(table_id)
    steps.append({"step": "define_table", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "storage-optimize", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 合并分区
    svc_merge = service_for("partition_merge")
    if not svc_merge:
        raise HTTPException(404, "partition_merge 服务未注册")
    r2 = svc_merge.domain_merge_partitions(merge_id)
    steps.append({"step": "merge_partitions", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "storage-optimize", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 规划压缩
    svc_comp = service_for("compaction_task")
    if not svc_comp:
        raise HTTPException(404, "compaction_task 服务未注册")
    r3 = svc_comp.domain_plan_compaction(compaction_id)
    steps.append({"step": "plan_compaction", "ok": r3["ok"], "details": r3})
    if not r3["ok"]:
        return {"chain": "storage-optimize", "ok": False, "steps": steps, "error": r3.get("message")}

    # Step 4: 压缩表
    r4 = svc_comp.domain_compact_table(compaction_id)
    steps.append({"step": "compact_table", "ok": r4["ok"], "details": r4})

    return {"chain": "storage-optimize", "ok": True, "steps": steps, "summary": {
        "table_def": r1.get("ddl"),
        "merged_files": r2.get("merged_files"),
        "compaction_plan": r3.get("plan"),
        "compression_ratio": r4.get("compression_ratio"),
    }}


# ------------------------------------------------------------------
# 链路3: Schema演进
# schema_evolve→data_backfill→batch_merge→stream_checkpoint
# ------------------------------------------------------------------

@router.post("/schema-evolve")
def chain_schema_evolve(schema_id: int, backfill_id: int, merge_id: int, checkpoint_id: int) -> dict:
    """Schema演进链路：Schema演进 → 数据回填 → 批流合并 → 保存检查点。"""
    steps = []

    # Step 1: Schema演进
    svc_schema = service_for("stream_schema")
    if not svc_schema:
        raise HTTPException(404, "stream_schema 服务未注册")
    r1 = svc_schema.domain_evolve_schema(schema_id)
    steps.append({"step": "evolve_schema", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "schema-evolve", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 数据回填
    svc_backfill = service_for("backfill_pipeline")
    if not svc_backfill:
        raise HTTPException(404, "backfill_pipeline 服务未注册")
    r2 = svc_backfill.domain_backfill(backfill_id)
    steps.append({"step": "data_backfill", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "schema-evolve", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 批流合并
    svc_merge = service_for("batch_merge")
    if not svc_merge:
        raise HTTPException(404, "batch_merge 服务未注册")
    r3 = svc_merge.domain_merge_batch_stream(merge_id)
    steps.append({"step": "batch_stream_merge", "ok": r3["ok"], "details": r3})
    if not r3["ok"]:
        return {"chain": "schema-evolve", "ok": False, "steps": steps, "error": r3.get("message")}

    # Step 4: 保存检查点
    svc_ckpt = service_for("stream_checkpoint")
    if not svc_ckpt:
        raise HTTPException(404, "stream_checkpoint 服务未注册")
    r4 = svc_ckpt.domain_save_checkpoint(checkpoint_id)
    steps.append({"step": "save_checkpoint", "ok": r4["ok"], "details": r4})

    return {"chain": "schema-evolve", "ok": True, "steps": steps, "summary": {
        "schema_version": r1.get("evolved_version"),
        "backfill_rows": r2.get("completed_rows"),
        "reconciled_total": r3.get("reconciled_total"),
        "checkpoint": r4.get("checkpoint_id"),
    }}


# ------------------------------------------------------------------
# 链路4: 生命周期
# snapshot_mgr→history_clean→retention_policy
# ------------------------------------------------------------------

@router.post("/lifecycle")
def chain_lifecycle(snapshot_id: int, clean_id: int, retention_id: int) -> dict:
    """生命周期链路：创建快照 → 清理历史 → 检查保留策略。"""
    steps = []

    # Step 1: 创建快照
    svc_snap = service_for("snapshot_mgr")
    if not svc_snap:
        raise HTTPException(404, "snapshot_mgr 服务未注册")
    r1 = svc_snap.domain_create_snapshot(snapshot_id)
    steps.append({"step": "create_snapshot", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "lifecycle", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 清理历史
    svc_vacuum = service_for("vacuum_job")
    if not svc_vacuum:
        raise HTTPException(404, "vacuum_job 服务未注册")
    r2 = svc_vacuum.domain_clean_history(clean_id)
    steps.append({"step": "clean_history", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "lifecycle", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 检查保留策略
    svc_ret = service_for("retention_policy")
    if not svc_ret:
        raise HTTPException(404, "retention_policy 服务未注册")
    r3 = svc_ret.domain_check_retention(retention_id)
    steps.append({"step": "check_retention", "ok": r3["ok"], "details": r3})

    return {"chain": "lifecycle", "ok": True, "steps": steps, "summary": {
        "snapshot_rows": r1.get("row_count"),
        "files_cleaned": r2.get("files_cleaned"),
        "can_clean_snapshots": r3.get("can_clean_count"),
    }}


# ------------------------------------------------------------------
# 链路5: 元数据治理
# lake_lineage→lake_audit→catalog_sync
# ------------------------------------------------------------------

@router.post("/metadata-govern")
def chain_metadata_govern(lineage_id: int, audit_id: int, catalog_id: int) -> dict:
    """元数据治理链路：追踪血缘 → 运行审计 → 同步目录。"""
    steps = []

    # Step 1: 追踪血缘
    svc_lineage = service_for("lake_lineage")
    if not svc_lineage:
        raise HTTPException(404, "lake_lineage 服务未注册")
    r1 = svc_lineage.domain_trace_lineage(lineage_id)
    steps.append({"step": "trace_lineage", "ok": r1["ok"], "details": r1})
    if not r1["ok"]:
        return {"chain": "metadata-govern", "ok": False, "steps": steps, "error": r1.get("message")}

    # Step 2: 运行审计
    svc_audit = service_for("lake_audit")
    if not svc_audit:
        raise HTTPException(404, "lake_audit 服务未注册")
    r2 = svc_audit.domain_run_audit(audit_id)
    steps.append({"step": "run_audit", "ok": r2["ok"], "details": r2})
    if not r2["ok"]:
        return {"chain": "metadata-govern", "ok": False, "steps": steps, "error": r2.get("message")}

    # Step 3: 同步目录
    svc_catalog = service_for("table_catalog")
    if not svc_catalog:
        raise HTTPException(404, "table_catalog 服务未注册")
    r3 = svc_catalog.domain_sync_catalog(catalog_id)
    steps.append({"step": "sync_catalog", "ok": r3["ok"], "details": r3})

    return {"chain": "metadata-govern", "ok": True, "steps": steps, "summary": {
        "lineage_depth": r1.get("depth"),
        "audit_tables": r2.get("tables_checked"),
        "synced_tables": r3.get("synced_tables"),
    }}

# ------------------------------------------------------------------
# 链路6: 批量回填
# backfill_arrange→plan_confirm→merge_queue→batch_merge_receive
# ------------------------------------------------------------------

class BackfillBatchIn(BaseModel):
    table: str
    requests: list[dict] | None = None
    plan_id: str | None = None
    completed_partitions: list[str] | None = None
    receipt_id: str | None = None


@router.post("/backfill-batch")
def chain_backfill_batch(payload: BackfillBatchIn) -> dict:
    """批量回填链路：安排 → 确认 → 刷新合并队列 → 批流合并接收完成分区。

    - 传 requests 时先安排一次批量回填（多条原请求同批提交）；
    - 也可以直接传已存在的 plan_id 继续后半段；
    - completed_partitions 为空时只走到「刷新合并队列」这一步。
    """
    from app.services.backfill_plan import BackfillPlanError, backfill_repository

    table = payload.table
    requests = payload.requests
    plan_id = payload.plan_id
    completed_partitions = payload.completed_partitions
    receipt_id = payload.receipt_id
    steps: list[dict] = []

    if plan_id is None:
        if not requests:
            raise HTTPException(400, "需要提供 requests 或 plan_id")
        try:
            arranged = backfill_repository.submit_plan({
                "name": "批量回填链路",
                "table": table,
                "requests": requests,
            })
        except BackfillPlanError as error:
            raise HTTPException(400, str(error)) from error
        plan_id = arranged["plan"]["plan_id"]
        steps.append({
            "step": "arrange_backfill",
            "ok": True,
            "details": {
                "plan_id": plan_id,
                "duplicate_submissions": arranged["duplicate_submissions"],
                "coverage": arranged["plan"]["coverage"],
                "intersection": arranged["queue"]["intersection"]["dispositions"],
            },
        })

    confirmed, message, already = backfill_repository.confirm_plan(plan_id)
    steps.append({
        "step": "confirm_plan",
        "ok": confirmed is not None and "仍需裁决" not in message,
        "details": {"message": message, "already_confirmed": already},
    })
    if confirmed is None or "仍需裁决" in message:
        return {"chain": "backfill-batch", "ok": False, "steps": steps, "error": message}

    queue = backfill_repository.merge_queue_item(plan_id)
    steps.append({
        "step": "merge_queue",
        "ok": queue is not None,
        "details": {
            "status": queue["status"] if queue else None,
            "next_committable": queue["next_committable_partitions"] if queue else [],
        },
    })

    if completed_partitions:
        svc_merge = service_for("batch_merge")
        if not svc_merge:
            raise HTTPException(404, "batch_merge 服务未注册")
        received = svc_merge.domain_receive_backfill_plan(
            1, plan_id, completed_partitions, receipt_id=receipt_id)
        steps.append({
            "step": "batch_merge_receive",
            "ok": received["ok"],
            "details": {
                "applied_count": received.get("applied_count"),
                "duplicate_count": received.get("duplicate_count"),
                "rejected_count": received.get("rejected_count"),
                "results": received.get("results"),
            },
        })
        return {
            "chain": "backfill-batch",
            "ok": received["ok"],
            "steps": steps,
            "summary": {
                "plan_id": plan_id,
                "received": received.get("plan_status"),
                "writeback_queue": received.get("writeback_queue", {}).get("status"),
            },
        }

    return {
        "chain": "backfill-batch",
        "ok": True,
        "steps": steps,
        "summary": {"plan_id": plan_id, "next_committable": queue["next_committable_partitions"] if queue else []},
    }
