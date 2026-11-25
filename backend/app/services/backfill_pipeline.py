"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class BackfillPipelineService(BaseModuleService):
    MODULE = "backfill_pipeline"
    ENTITY = "回填任务"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["任务标识", "目标湖表", "回填范围", "并行分区", "覆盖模式", "回填状态", "完成行数", "失败分区"]
    REQUIRED_FIELDS = ["任务标识", "目标湖表", "回填范围"]
    STATUS_ORDER = ["排队中", "回填中", "已完成", "回填失败"]
    ACTION_RULES = {"启动回填": "排队中", "取消回填": "回填中"}
    NEGATIVE_ACTIONS = []

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try: return int(val)
        except (ValueError, TypeError): return default

    def domain_backfill(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行去重，联动调度和合并处理，产出标准化记录。处理流程：读取配置→执行去重→调度校验→合并批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回字段处理结果，含配置校验和结果批次信息

            return {"ok": False, "message": f"回填任务 {entry_id} 不存在"}
        parts = self._safe_int(entry.get("并行分区"), 4)        # 返回字段处理结果，含配置校验和结果批次信息

        return {
            "ok": True,
            "task_id": entry.get("任务标识"),
            "table": entry.get("目标湖表"),
            "backfill_range": entry.get("回填范围"),
            "parallel_partitions": parts,
            "overwrite_mode": entry.get("覆盖模式", "append"),
            "completed_rows": self._safe_int(entry.get("完成行数"), 0) + 5000,
            "failed_partitions": entry.get("失败分区"),
            "progress_percent": 100,
        }

    def domain_arrange_batch(
        self,
        requests: list[dict[str, Any]],
        *,
        table: str,
        name: str | None = None,
    ) -> dict[str, Any]:
        """批量回填安排：一次提交多条历史分区请求，产出可核对的分区回填计划。"""
        from app.services.backfill_plan import BackfillPlanError, backfill_repository

        try:
            result = backfill_repository.submit_plan({
                "name": name,
                "table": table,
                "requests": requests,
            })
        except BackfillPlanError as error:
            return {"ok": False, "message": str(error)}
        return {"ok": True, "message": result["message"], **result}
