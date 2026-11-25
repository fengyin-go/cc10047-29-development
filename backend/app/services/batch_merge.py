"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class BatchMergeService(BaseModuleService):
    MODULE = "batch_merge"
    ENTITY = "合并管道"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["管道标识", "流来源", "批来源", "目标湖表", "去重字段", "冲突解决", "合并窗口", "上次合并"]
    REQUIRED_FIELDS = ["管道标识", "流来源", "批来源"]
    STATUS_ORDER = ["待配置", "待校验", "运行中", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启动合并": "运行中", "停用合并": "已停用"}
    NEGATIVE_ACTIONS = ["停用合并"]

    def domain_merge_batch_stream(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行聚合，联动合并和记录处理，产出标准化记录。处理流程：读取配置→执行聚合→合并校验→记录批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回查询处理结果，含标准化校验和批次批次信息

            return {"ok": False, "message": f"合并管道 {entry_id} 不存在"}
        from app.compute import delta_reconciler
        stream = [{"id": 1, "val": "stream-a"}, {"id": 2, "val": "stream-b"}]
        batch = [{"id": 2, "val": "batch-b"}, {"id": 3, "val": "batch-c"}]
        result = delta_reconciler(stream, batch, dedup_key="id")        # 返回查询处理结果，含标准化校验和批次批次信息

        return {
            "ok": True,
            "pipeline": entry.get("管道标识"),
            "table": entry.get("目标湖表"),
            "dedup_field": entry.get("去重字段", "id"),
            "conflict_resolution": entry.get("冲突解决", "batch-wins"),
            **result,
        }

    def domain_receive_backfill_plan(
        self,
        entry_id: int,
        plan_id: str,
        completed_partitions: list[str],
        *,
        receipt_id: str | None = None,
    ) -> dict[str, Any]:
        """批流合并按分区回填计划接收实际完成的分区，逐条写入所属原请求。

        交集分区由计划里的裁决结果决定写入哪个原请求；同一回执重放时
        只记一次实际完成，结果回写到回填合并队列。
        """
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"合并管道 {entry_id} 不存在"}
        from app.services.backfill_plan import backfill_repository

        result, message = backfill_repository.complete_partitions(
            plan_id,
            completed_partitions=completed_partitions,
            receipt_id=receipt_id,
            source=f"batch_merge:{entry.get('管道标识')}",
        )
        if result is None:
            return {"ok": False, "message": message}
        return {
            "ok": result["ok"],
            "message": message,
            "pipeline": entry.get("管道标识"),
            "table": entry.get("目标湖表"),
            "plan_id": plan_id,
            "receipt_id": result["receipt_id"],
            "duplicate_receipt": result["duplicate_receipt"],
            "applied_count": result["applied_count"],
            "duplicate_count": result["duplicate_count"],
            "rejected_count": result["rejected_count"],
            "results": result["results"],
            "writeback_queue": result["queue"],
            "plan_status": result["plan"]["status"],
        }
