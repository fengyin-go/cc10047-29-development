"""分区回填计划：批量回填安排、交集裁决、回填合并队列与幂等接收。

设计要点
--------
1. 一次提交可以带多个原请求（例如 p1~p3 追加、p2~p4 替换），每个原请求保留
   自己的覆盖模式、优先级和范围。
2. 计划按分区展开：独立分区只属于一个原请求；交集分区保留全部候选原请求，
   由覆盖模式优先级裁决去向；优先级相同且模式冲突时进入「待裁决」，交给
   回填合并队列处理。
3. 批流合并按计划接收「实际完成的分区」，逐条写入所属原请求。写入以
   (目标湖表, 分区) 为唯一键，同名分区按目标湖表隔离；重复回执和断线重放
   只记一次实际完成，保证并发确认/恢复不会把交集写两次。
"""
from __future__ import annotations

import copy
import re
import threading
from datetime import datetime, timezone
from typing import Any

MODE_ALIASES = {
    "append": "append",
    "追加": "append",
    "追加写入": "append",
    "replace": "replace",
    "overwrite": "replace",
    "覆盖": "replace",
    "替换": "replace",
    "覆盖写入": "replace",
}
MODE_LABELS = {"append": "追加", "replace": "替换"}
# 未显式给出优先级时，替换高于追加：覆盖要求的默认优先级决定交集去向。
DEFAULT_MODE_PRIORITY = {"append": 1, "replace": 2}

PARTITION_STATUS_PENDING = "待提交"
PARTITION_STATUS_NEED_DECISION = "待裁决"
PARTITION_STATUS_RECEIVED = "已接收"

PLAN_STATUS_DRAFT = "待确认"
PLAN_STATUS_CONFIRMED = "已确认"
PLAN_STATUS_PARTIAL = "部分完成"
PLAN_STATUS_DONE = "已完成"

_NUMERIC_SUFFIX = re.compile(r"^(.*?)(\d+)$")
_RANGE_SEPARATORS = ("~", "..", "到", "至")


class BackfillPlanError(ValueError):
    """计划参数不合法。"""


# ----------------------------------------------------------------------
# 纯函数：分区枚举、模式归一、计划构建
# ----------------------------------------------------------------------

def normalize_mode(value: Any) -> str:
    text = str(value or "").strip().casefold()
    mode = MODE_ALIASES.get(text)
    if mode is None:
        raise BackfillPlanError(f"不支持的覆盖模式：{value}（可选：追加/替换）")
    return mode


def mode_label(mode: str) -> str:
    return MODE_LABELS.get(mode, mode)


def enumerate_partitions(start: Any, end: Any = None) -> list[str]:
    """把起止分区展开成严格有序的分区列表。

    支持 p1/p3 → p1,p2,p3；2026-01-01/2026-01-03 这类同前缀数字后缀；
    逗号分隔的显式列表；也支持 "p1~p3"、"p1..p3"、"p1 到 p3" 这种单一区间串。
    """
    start_text = str(start or "").strip()
    end_text = str(end or "").strip()
    if not start_text:
        raise BackfillPlanError("回填起始分区不能为空")
    if end_text in ("", "—", "-"):
        end_text = start_text
    if not end_text:
        end_text = start_text

    # 单字段里直接给了区间：p1~p3 / p1..p3 / p1 到 p3
    if end_text == start_text:
        for separator in _RANGE_SEPARATORS:
            if separator in start_text:
                left, right = start_text.split(separator, 1)
                if left.strip() and right.strip():
                    return enumerate_partitions(left, right)
                raise BackfillPlanError(f"无法识别回填范围：{start_text}")

    if "," in start_text or "，" in start_text:
        parts = [item.strip() for item in re.split(r"[,，]", start_text) if item.strip()]
        if not parts:
            raise BackfillPlanError("回填范围不能为空")
        return _dedupe(parts)

    start_match = _NUMERIC_SUFFIX.match(start_text)
    end_match = _NUMERIC_SUFFIX.match(end_text)
    if (
        start_match
        and end_match
        and start_match.group(1) == end_match.group(1)
    ):
        prefix = start_match.group(1)
        start_num_text, end_num_text = start_match.group(2), end_match.group(2)
        start_num, end_num = int(start_num_text), int(end_num_text)
        if end_num < start_num:
            raise BackfillPlanError(f"回填结束分区 {end_text} 早于起始分区 {start_text}")
        width = len(start_num_text) if start_num_text.startswith("0") else 0
        return [f"{prefix}{str(num).zfill(width)}" for num in range(start_num, end_num + 1)]

    if start_text == end_text:
        return [start_text]
    return [start_text, end_text]


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def build_request(raw: dict[str, Any], table: str, sequence: int) -> dict[str, Any]:
    """把一条原请求标准化，保留它自己的覆盖要求、优先级与范围。

    除了请求级 mode，还支持 partition_modes（分区 -> 模式）为单个分区
    指定不同覆盖模式；未显式给优先级时按覆盖模式取默认优先级
   （替换高于追加），因此覆盖要求本身就决定交集去向。
    """
    start = raw.get("start_partition") or raw.get("start") or raw.get("回填起始分区")
    end = raw.get("end_partition") or raw.get("end") or raw.get("回填结束分区")
    if not start and raw.get("range"):
        start = raw.get("range")
        end = raw.get("range")
    partitions = enumerate_partitions(start, end)
    mode = normalize_mode(raw.get("mode") or raw.get("覆盖模式") or "append")
    priority_raw = raw.get("priority", raw.get("优先级"))
    priority = _safe_int(priority_raw, DEFAULT_MODE_PRIORITY[mode])
    request_id = str(raw.get("request_id") or raw.get("原请求") or "").strip() or None
    label = str(raw.get("label") or raw.get("请求名称") or "").strip() or None
    partition_modes_raw = raw.get("partition_modes") or raw.get("分区覆盖模式") or {}
    partition_modes: dict[str, str] = {}
    if isinstance(partition_modes_raw, dict):
        for key, value in partition_modes_raw.items():
            name = str(key).strip()
            if name:
                partition_modes[name] = normalize_mode(value)
    return {
        "request_id": request_id,
        "label": label,
        "table": table,
        "range": f"{partitions[0]}~{partitions[-1]}" if len(partitions) > 1 else partitions[0],
        "start_partition": partitions[0],
        "end_partition": partitions[-1],
        "partitions": partitions,
        "mode": mode,
        "mode_label": mode_label(mode),
        "priority": priority,
        "priority_explicit": priority_raw is not None,
        "partition_modes": partition_modes,
        "rows": _safe_int(raw.get("rows", raw.get("完成行数")), 0),
        "dedup_key": (
            f"{table}|{partitions[0]}|{partitions[-1]}|{mode}|{priority}"
            f"|{sorted(partition_modes.items())}"
        ),
        "duplicate_submissions": 0,
    }


def effective_requirement(request: dict[str, Any], partition: str) -> tuple[str, int]:
    """返回原请求在某个分区上的实际覆盖模式与优先级。"""
    mode = request.get("partition_modes", {}).get(partition, request["mode"])
    if request.get("priority_explicit"):
        priority = request["priority"]
    else:
        priority = DEFAULT_MODE_PRIORITY[mode]
    return mode, priority



def adjudicate_intersection(owners: list[dict[str, Any]]) -> dict[str, Any]:
    """交集裁决：覆盖模式优先级高者胜；同优先级且模式冲突则待裁决。"""
    modes = {owner["mode"] for owner in owners}
    candidates = [
        {
            "request_id": owner["request_id"],
            "mode": owner["mode"],
            "mode_label": owner["mode_label"],
            "priority": owner["priority"],
        }
        for owner in owners
    ]
    base = {
        "candidates": candidates,
        "candidate_modes": sorted(modes),
        "source_lookback_required": True,
    }
    if len(modes) == 1:
        winner = max(owners, key=lambda item: (item["priority"], -owners.index(item)))
        return {
            **base,
            "resolved": True,
            "winner_request": winner["request_id"],
            "mode": winner["mode"],
            "mode_label": winner["mode_label"],
            "rule": "same-mode",
            "reason": (
                f"交集原请求覆盖模式一致（{winner['mode_label']}），"
                f"按优先级 {winner['priority']} 归并，只写一次"
            ),
        }

    top_priority = max(owner["priority"] for owner in owners)
    top = [owner for owner in owners if owner["priority"] == top_priority]
    top_modes = {owner["mode"] for owner in top}
    if len(top) == 1:
        winner = top[0]
        lower = [owner for owner in owners if owner is not winner]
        reason = (
            f"{winner['request_id']} 的覆盖模式优先级 {winner['priority']} 高于 "
            + "、".join(
                f"{owner['request_id']}(优先级 {owner['priority']})" for owner in lower
            )
            + f"，交集按{winner['mode_label']}执行"
        )
        return {
            **base,
            "resolved": True,
            "winner_request": winner["request_id"],
            "mode": winner["mode"],
            "mode_label": winner["mode_label"],
            "rule": "priority",
            "reason": reason,
        }
    if len(top_modes) == 1:
        winner = top[0]
        return {
            **base,
            "resolved": True,
            "winner_request": winner["request_id"],
            "mode": winner["mode"],
            "mode_label": winner["mode_label"],
            "rule": "priority-same-mode",
            "reason": "交集原请求优先级相同且覆盖模式一致，归并后只写一次",
        }
    return {
        **base,
        "resolved": False,
        "winner_request": None,
        "mode": None,
        "mode_label": None,
        "rule": "manual",
        "reason": "交集覆盖模式冲突且优先级相同，需在回填合并队列中裁决",
    }


def build_partitions(
    requests: list[dict[str, Any]], table: str,
) -> list[dict[str, Any]]:
    """按目标湖表 + 分区名展开覆盖关系，同名分区跨表互不影响。"""
    ordered: list[str] = []
    for request in requests:
        for partition in request["partitions"]:
            if partition not in ordered:
                ordered.append(partition)

    partitions: list[dict[str, Any]] = []
    for partition in ordered:
        owners = []
        for request in requests:
            if partition not in request["partitions"]:
                continue
            mode, priority = effective_requirement(request, partition)
            owners.append({
                "request_id": request["request_id"],
                "label": request.get("label"),
                "range": request["range"],
                "mode": mode,
                "mode_label": mode_label(mode),
                "priority": priority,
            })
        if len(owners) == 1:
            partition_entry = {
                "partition": partition,
                "table": table,
                "kind": "独立",
                "owners": owners,
                "adjudication": None,
                "status": PARTITION_STATUS_PENDING,
                "received": None,
            }
        else:
            adjudication = adjudicate_intersection(owners)
            partition_entry = {
                "partition": partition,
                "table": table,
                "kind": "交集",
                "owners": owners,
                "adjudication": adjudication,
                "status": (
                    PARTITION_STATUS_PENDING
                    if adjudication["resolved"]
                    else PARTITION_STATUS_NEED_DECISION
                ),
                "received": None,
            }
        partitions.append(partition_entry)
    return partitions


def plan_signature(
    table: str, name: str, requests: list[dict[str, Any]],
) -> str:
    """相同范围（表 + 起止分区 + 覆盖模式 + 优先级）的重复提交视为同一计划。"""
    pieces = sorted(item["dedup_key"] for item in requests)
    return f"{table}::" + ";".join(pieces)


def build_plan(
    *,
    plan_id: str,
    name: str,
    table: str,
    requests: list[dict[str, Any]],
    signature: str,
) -> dict[str, Any]:
    return {
        "plan_id": plan_id,
        "name": name,
        "table": table,
        "status": PLAN_STATUS_DRAFT,
        "created_at": _now(),
        "confirmed_at": None,
        "confirm_token": None,
        "confirm_attempts": 0,
        "signature": signature,
        "requests": requests,
        "partitions": build_partitions(requests, table),
        "receipts": [],
        "duplicate_submissions": 0,
    }


def plan_view(plan: dict[str, Any]) -> dict[str, Any]:
    """生成可核对的计划视图：覆盖范围、交集裁决、已接收与待处理分区。"""
    requests = plan["requests"]
    coverage = {request["request_id"]: list(request["partitions"]) for request in requests}
    non_intersection_scope: dict[str, list[str]] = {request["request_id"]: [] for request in requests}
    intersection_scope: dict[str, list[str]] = {request["request_id"]: [] for request in requests}
    intersection_partitions: list[str] = []
    pending_partitions: list[str] = []
    received_partitions: list[str] = []
    need_decision: list[str] = []
    next_committable: dict[str, list[str]] = {request["request_id"]: [] for request in requests}

    for entry in plan["partitions"]:
        partition = entry["partition"]
        owner_ids = [owner["request_id"] for owner in entry["owners"]]
        if entry["kind"] == "交集":
            intersection_partitions.append(partition)
            winner = (entry.get("adjudication") or {}).get("winner_request")
            if winner:
                intersection_scope.setdefault(winner, []).append(partition)
        else:
            non_intersection_scope.setdefault(owner_ids[0], []).append(partition)

        if entry["status"] == PARTITION_STATUS_RECEIVED:
            received_partitions.append(partition)
        else:
            pending_partitions.append(partition)
            if entry["status"] == PARTITION_STATUS_NEED_DECISION:
                need_decision.append(partition)
            else:
                target = (
                    (entry.get("adjudication") or {}).get("winner_request")
                    if entry["kind"] == "交集"
                    else owner_ids[0]
                )
                if target:
                    next_committable.setdefault(target, []).append(partition)

    return {
        "plan_id": plan["plan_id"],
        "name": plan["name"],
        "table": plan["table"],
        "status": plan["status"],
        "created_at": plan["created_at"],
        "confirmed_at": plan["confirmed_at"],
        "requests": copy.deepcopy(requests),
        "partitions": copy.deepcopy(plan["partitions"]),
        "coverage": coverage,
        "non_intersection_scope": non_intersection_scope,
        "intersection_scope": intersection_scope,
        "intersection_partitions": intersection_partitions,
        "pending_partitions": pending_partitions,
        "received_partitions": received_partitions,
        "need_decision_partitions": need_decision,
        "next_committable": next_committable,
        "receipts": copy.deepcopy(plan["receipts"]),
        "duplicate_submissions": plan["duplicate_submissions"],
    }


def build_queue_item(plan: dict[str, Any]) -> dict[str, Any]:
    """回填合并队列条目：保留交集裁决、两个原请求和非交集可提交范围。"""
    view = plan_view(plan)
    requests = view["requests"]
    intersection_entries = [
        entry for entry in view["partitions"] if entry["kind"] == "交集"
    ]
    dispositions = []
    for entry in intersection_entries:
        disposition = copy.deepcopy(entry.get("adjudication") or {})
        disposition["partition"] = entry["partition"]
        disposition["status"] = entry["status"]
        disposition["source_lookback"] = _lookback_sources(plan, entry["partition"])
        dispositions.append(disposition)
    unresolved = [entry["partition"] for entry in intersection_entries if not (entry.get("adjudication") or {}).get("resolved")]

    if unresolved:
        queue_status = "待裁决"
    elif view["status"] == PLAN_STATUS_DONE:
        queue_status = "已完成"
    elif view["received_partitions"]:
        queue_status = "部分接收"
    elif view["status"] == PLAN_STATUS_CONFIRMED:
        queue_status = "待提交"
    else:
        queue_status = "待确认"

    confirmed = bool(plan.get("confirmed_at"))
    return {
        "queue_id": f"MQ-{plan['plan_id'].split('-')[-1]}",
        "plan_id": plan["plan_id"],
        "name": plan["name"],
        "table": plan["table"],
        "status": queue_status,
        "confirmed": confirmed,
        "confirmation": {
            "confirmed": confirmed,
            "confirmed_at": plan.get("confirmed_at"),
            "attempts": plan.get("confirm_attempts", 0),
            "token": plan.get("confirm_token"),
        },
        "intersection": {
            "partitions": view["intersection_partitions"],
            "unresolved_partitions": unresolved,
            "status": "待裁决" if unresolved else (
                "已裁决" if intersection_entries else "无交集"
            ),
            "dispositions": dispositions,
            "source_lookback_required": True,
        },
        "original_requests": [
            {
                "request_id": request["request_id"],
                "label": request.get("label"),
                "range": request["range"],
                "start_partition": request["start_partition"],
                "end_partition": request["end_partition"],
                "mode": request["mode"],
                "mode_label": request["mode_label"],
                "partition_modes": request.get("partition_modes", {}),
                "priority": request["priority"],
                "coverage": view["coverage"].get(request["request_id"], []),
                "non_intersection_scope": view["non_intersection_scope"].get(request["request_id"], []),
                "intersection_scope": view["intersection_scope"].get(request["request_id"], []),
            }
            for request in requests
        ],
        "non_intersection_scope": view["non_intersection_scope"],
        "committable_scope": view["non_intersection_scope"],
        "intersection_scope": view["intersection_scope"],
        "pending_partitions": view["pending_partitions"],
        "received_partitions": view["received_partitions"],
        "next_committable": view["next_committable"],
        "next_committable_partitions": [
            partition
            for partitions in view["next_committable"].values()
            for partition in partitions
        ],
        "writebacks": copy.deepcopy(plan.get("writebacks", [])),
    }


def _lookback_sources(plan: dict[str, Any], partition: str) -> list[dict[str, Any]]:
    """一组里有共同分区时，逐条回查交集涉及的每一个原请求来源。"""
    sources: list[dict[str, Any]] = []
    for entry in plan["partitions"]:
        if entry["partition"] != partition:
            continue
        for owner in entry["owners"]:
            sources.append({
                "source": f"原请求 {owner['request_id']}",
                "request_id": owner["request_id"],
                "mode": owner["mode"],
                "mode_label": owner["mode_label"],
                "range": owner["range"],
                "found": partition in _request_partitions(plan, owner["request_id"]),
            })
    return sources


def _request_partitions(plan: dict[str, Any], request_id: str) -> list[str]:
    for request in plan["requests"]:
        if request["request_id"] == request_id:
            return request["partitions"]
    return []


# ----------------------------------------------------------------------
# 仓库：带锁的幂等状态，支持并发确认与断线重放
# ----------------------------------------------------------------------

class BackfillPlanRepository:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._plans: dict[str, dict[str, Any]] = {}
        self._plan_order: list[str] = []
        self._requests: dict[str, dict[str, Any]] = {}
        self._request_by_dedup: dict[str, str] = {}
        self._completions: dict[tuple[str, str], dict[str, Any]] = {}
        # 回执按 (计划, 回执号) 去重：不同计划的同名回执互不影响。
        self._receipts: dict[tuple[str, str], dict[str, Any]] = {}
        self._plan_sequence = 0
        self._request_sequence = 0

    # -- 提交 ---------------------------------------------------------
    def submit_plan(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            name = str(payload.get("name") or payload.get("计划名称") or "").strip()
            table = str(payload.get("table") or payload.get("目标湖表") or "").strip()
            raw_requests = payload.get("requests") or payload.get("原请求") or []
            if not table:
                raise BackfillPlanError("目标湖表不能为空")
            if not isinstance(raw_requests, list) or not raw_requests:
                raise BackfillPlanError("批量回填至少需要一条原请求")

            requests: list[dict[str, Any]] = []
            duplicate_submissions = 0
            seen_in_batch: set[str] = set()
            for index, raw in enumerate(raw_requests, start=1):
                if not isinstance(raw, dict):
                    raise BackfillPlanError(f"第 {index} 条原请求格式不正确")
                request = build_request(raw, table, index)
                dedup_key = request["dedup_key"]
                if dedup_key in seen_in_batch:
                    duplicate_submissions += 1
                    continue
                seen_in_batch.add(dedup_key)
                existing_id = self._request_by_dedup.get(dedup_key)
                if existing_id and existing_id in self._requests:
                    # 相同范围重复提交：复用原请求，不重复登记完成
                    existing = copy.deepcopy(self._requests[existing_id])
                    existing["duplicate_submissions"] = (
                        int(existing.get("duplicate_submissions", 0)) + 1
                    )
                    self._requests[existing_id]["duplicate_submissions"] = existing["duplicate_submissions"]
                    duplicate_submissions += 1
                    request = existing
                else:
                    request["request_id"] = request["request_id"] or self._next_request_id()
                    self._request_sequence = max(
                        self._request_sequence,
                        _safe_int(request["request_id"].split("-")[-1], self._request_sequence),
                    )
                    self._requests[request["request_id"]] = copy.deepcopy(request)
                    self._request_by_dedup[dedup_key] = request["request_id"]
                requests.append(request)

            if not requests:
                raise BackfillPlanError("批量回填没有可执行的原请求")

            signature = plan_signature(table, name, requests)
            existing_plan = self._find_by_signature(signature)
            if existing_plan is not None:
                return {
                    "created": False,
                    "deduplicated": True,
                    "duplicate_submissions": duplicate_submissions,
                    "plan": plan_view(existing_plan),
                    "queue": build_queue_item(existing_plan),
                    "message": f"相同范围的批量回填已存在，复用计划 {existing_plan['plan_id']}，实际完成只记一次",
                }

            self._plan_sequence += 1
            plan_id = f"PLAN-{self._plan_sequence:03d}"
            display_name = name or f"批量回填计划 {self._plan_sequence:03d}"
            plan = build_plan(
                plan_id=plan_id,
                name=display_name,
                table=table,
                requests=requests,
                signature=signature,
            )
            plan["duplicate_submissions"] = duplicate_submissions
            self._plans[plan_id] = plan
            self._plan_order.append(plan_id)
            queue = build_queue_item(plan)
            return {
                "created": True,
                "deduplicated": False,
                "duplicate_submissions": duplicate_submissions,
                "plan": plan_view(plan),
                "queue": queue,
                "message": f"批量回填计划 {plan_id} 已生成，待确认后进入回填合并队列",
            }

    def _find_by_signature(self, signature: str) -> dict[str, Any] | None:
        for plan_id in self._plan_order:
            plan = self._plans[plan_id]
            if plan["signature"] == signature and plan["status"] != PLAN_STATUS_DONE:
                return plan
        return None

    def _next_request_id(self) -> str:
        self._request_sequence += 1
        return f"REQ-{self._request_sequence:03d}"

    # -- 查询 ---------------------------------------------------------
    def list_plans(self) -> dict[str, Any]:
        with self._lock:
            items = [self._plan_summary(self._plans[plan_id]) for plan_id in self._plan_order]
            return {"items": items, "total": len(items)}

    def plan_detail(self, plan_id: str) -> dict[str, Any] | None:
        with self._lock:
            plan = self._plans.get(plan_id)
            if plan is None:
                return None
            return {
                "plan": plan_view(plan),
                "queue": build_queue_item(plan),
            }

    def merge_queue(self) -> dict[str, Any]:
        with self._lock:
            items = [
                build_queue_item(self._plans[plan_id])
                for plan_id in self._plan_order
            ]
            return {"items": items, "total": len(items)}

    def merge_queue_item(self, plan_id: str) -> dict[str, Any] | None:
        with self._lock:
            plan = self._plans.get(plan_id)
            if plan is None:
                return None
            return build_queue_item(plan)

    def _plan_summary(self, plan: dict[str, Any]) -> dict[str, Any]:
        view = plan_view(plan)
        return {
            "plan_id": view["plan_id"],
            "name": view["name"],
            "table": view["table"],
            "status": view["status"],
            "coverage": view["coverage"],
            "pending_partitions": view["pending_partitions"],
            "received_partitions": view["received_partitions"],
            "intersection_partitions": view["intersection_partitions"],
            "confirmed": bool(plan.get("confirmed_at")),
        }

    # -- 确认 ---------------------------------------------------------
    def confirm_plan(
        self, plan_id: str, *, token: str | None = None, operator: str | None = None,
    ) -> tuple[dict[str, Any] | None, str, bool]:
        """确认计划。并发确认只在第一次落定，后续调用幂等返回同一份结果。"""
        with self._lock:
            plan = self._plans.get(plan_id)
            if plan is None:
                return None, f"分区回填计划 {plan_id} 不存在", False

            if plan.get("confirmed_at"):
                plan["confirm_attempts"] = int(plan.get("confirm_attempts", 0)) + 1
                return {
                    "plan": plan_view(plan),
                    "queue": build_queue_item(plan),
                    "already_confirmed": True,
                }, "计划已确认，本次为幂等确认，未重复生成队列或写入", True

            unresolved = [
                entry["partition"]
                for entry in plan["partitions"]
                if entry["status"] == PARTITION_STATUS_NEED_DECISION
            ]
            if unresolved:
                return {
                    "plan": plan_view(plan),
                    "queue": build_queue_item(plan),
                    "already_confirmed": False,
                }, f"交集分区 {'、'.join(unresolved)} 仍需裁决，暂不能确认", False

            if not plan["partitions"]:
                return None, "计划没有可执行的分区", False

            plan["status"] = PLAN_STATUS_CONFIRMED
            plan["confirmed_at"] = _now()
            plan["confirm_token"] = token or f"{plan_id}-{self._plan_sequence}"
            plan["confirm_attempts"] = 1
            plan["writebacks"] = plan.get("writebacks", [])
            return {
                "plan": plan_view(plan),
                "queue": build_queue_item(plan),
                "already_confirmed": False,
                "operator": operator or "数据工程组",
            }, f"计划 {plan_id} 已确认，回填合并队列已刷新", False

    # -- 交集裁决 ------------------------------------------------------
    def resolve_intersection(
        self,
        plan_id: str,
        *,
        winner_request: str,
        partitions: list[str] | None = None,
        operator: str | None = None,
        remark: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        with self._lock:
            plan = self._plans.get(plan_id)
            if plan is None:
                return None, f"分区回填计划 {plan_id} 不存在"
            if plan.get("confirmed_at"):
                return None, "计划已确认，交集裁决已冻结，不能再改"

            targets = partitions or [
                entry["partition"]
                for entry in plan["partitions"]
                if entry["kind"] == "交集"
                and not (entry.get("adjudication") or {}).get("resolved")
            ]
            if not targets:
                return None, "当前计划没有待裁决的交集分区"

            changed = False
            for entry in plan["partitions"]:
                if entry["partition"] not in targets:
                    continue
                owner = next(
                    (owner for owner in entry["owners"] if owner["request_id"] == winner_request),
                    None,
                )
                if owner is None:
                    return None, f"原请求 {winner_request} 未覆盖分区 {entry['partition']}"
                entry["adjudication"] = {
                    **(entry.get("adjudication") or {}),
                    "resolved": True,
                    "winner_request": winner_request,
                    "mode": owner["mode"],
                    "mode_label": owner["mode_label"],
                    "rule": "manual",
                    "reason": remark or f"回填合并队列人工裁决：{winner_request} 优先",
                    "decided_by": operator or "数据工程组",
                    "decided_at": _now(),
                }
                entry["status"] = PARTITION_STATUS_PENDING
                changed = True
            if not changed:
                return None, "指定分区不在当前计划中"
            return {
                "plan": plan_view(plan),
                "queue": build_queue_item(plan),
            }, f"交集分区 {'、'.join(targets)} 已裁决给 {winner_request}"

    # -- 接收实际完成分区（批流合并入口） ------------------------------
    def complete_partitions(
        self,
        plan_id: str,
        *,
        completed_partitions: list[str] | None = None,
        receipt_id: str | None = None,
        source: str = "batch_merge",
        sources: dict[str, list[dict[str, Any]]] | None = None,
        operator: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        with self._lock:
            plan = self._plans.get(plan_id)
            if plan is None:
                return None, f"分区回填计划 {plan_id} 不存在"
            if not plan.get("confirmed_at"):
                return None, "计划尚未确认，不能接收完成分区"

            receipt_key = (receipt_id or "").strip()
            receipt_cache_key = (plan_id, receipt_key)
            if receipt_key and receipt_cache_key in self._receipts:
                cached = copy.deepcopy(self._receipts[receipt_cache_key])
                cached["duplicate_receipt"] = True
                cached["original_applied_count"] = cached.get("applied_count", 0)
                cached["applied_count"] = 0
                cached["duplicate_count"] = cached.get("duplicate_count", 0) + len(
                    [item for item in cached.get("results", []) if item.get("status") == "已接收"]
                )
                cached["message"] = "同一回执已处理过，断线重放被幂等拦截，未重复写入"
                cached["plan"] = plan_view(plan)
                cached["queue"] = build_queue_item(plan)
                return cached, cached["message"]

            requested = completed_partitions or []
            if not requested:
                return None, "请提供实际完成的分区"
            if not isinstance(requested, list):
                return None, "实际完成分区必须是列表"

            results: list[dict[str, Any]] = []
            applied = 0
            duplicates = 0
            rejected = 0
            seen_in_receipt: set[str] = set()
            for raw_partition in requested:
                partition = str(raw_partition or "").strip()
                if not partition:
                    continue
                if partition in seen_in_receipt:
                    duplicates += 1
                    results.append({
                        "partition": partition,
                        "status": "已接收",
                        "duplicate": True,
                        "already_completed": True,
                        "reason": "同一回执里重复列出的分区只记一次实际完成",
                    })
                    continue
                seen_in_receipt.add(partition)
                entry = next(
                    (item for item in plan["partitions"] if item["partition"] == partition),
                    None,
                )
                if entry is None:
                    rejected += 1
                    results.append({
                        "partition": partition,
                        "status": "已拒绝",
                        "reason": f"分区 {partition} 不在计划覆盖范围内（按目标湖表 {plan['table']} 隔离）",
                    })
                    continue

                # 同名分区按目标湖表隔离：唯一键必须带表名
                completion_key = (plan["table"], partition)
                if completion_key in self._completions:
                    duplicates += 1
                    existing = self._completions[completion_key]
                    results.append({
                        "partition": partition,
                        "table": plan["table"],
                        "status": "已接收",
                        "duplicate": True,
                        "already_completed": True,
                        "written_to": existing["request_id"],
                        "mode": existing["mode"],
                        "mode_label": existing["mode_label"],
                        "reason": "该分区已记过一次实际完成，重复提交不再写入",
                    })
                    continue

                adjudication = entry.get("adjudication") or {}
                if entry["kind"] == "交集" and not adjudication.get("resolved"):
                    rejected += 1
                    results.append({
                        "partition": partition,
                        "status": "已拒绝",
                        "reason": "交集覆盖模式冲突且尚未裁决，等待回填合并队列裁决",
                    })
                    continue

                lookback = _lookback_sources(plan, partition) if entry["kind"] == "交集" else []
                provided = (sources or {}).get(partition)
                if provided:
                    for source_entry in provided:
                        lookback.append({
                            "source": source_entry.get("source", "外部来源"),
                            "request_id": source_entry.get("request_id"),
                            "found": bool(source_entry.get("found", False)),
                        })
                if entry["kind"] == "交集" and any(not item.get("found") for item in lookback):
                    rejected += 1
                    results.append({
                        "partition": partition,
                        "status": "已拒绝",
                        "reason": "交集分区回查来源不完整，未通过双来源核对",
                        "source_lookback": lookback,
                    })
                    continue

                owner_request = (
                    adjudication.get("winner_request")
                    if entry["kind"] == "交集"
                    else entry["owners"][0]["request_id"]
                )
                request = self._request(plan, owner_request)
                if entry["kind"] == "交集":
                    mode = adjudication.get("mode") or "append"
                elif request is not None:
                    mode, _priority = effective_requirement(request, partition)
                else:
                    mode = entry["owners"][0]["mode"]
                written = {
                    "partition": partition,
                    "table": plan["table"],
                    "request_id": owner_request,
                    "mode": mode,
                    "mode_label": mode_label(mode),
                    "receipt_id": receipt_key or None,
                    "source": source,
                    "source_lookback": lookback,
                    "at": _now(),
                    "rows": _safe_int((request or {}).get("rows"), 0),
                }
                self._completions[completion_key] = written
                entry["status"] = PARTITION_STATUS_RECEIVED
                entry["received"] = written
                if entry["kind"] == "交集":
                    entry["adjudication"] = {
                        **adjudication,
                        "source_lookback": lookback,
                        "written_request": owner_request,
                    }
                applied += 1
                results.append({
                    "partition": partition,
                    "table": plan["table"],
                    "status": "已接收",
                    "duplicate": False,
                    "written_to": owner_request,
                    "mode": mode,
                    "mode_label": mode_label(mode),
                    "kind": entry["kind"],
                    "source_lookback": lookback,
                })

            if applied:
                plan["status"] = (
                    PLAN_STATUS_DONE
                    if all(entry["status"] == PARTITION_STATUS_RECEIVED for entry in plan["partitions"])
                    else PLAN_STATUS_PARTIAL
                )
                plan.setdefault("writebacks", []).append({
                    "receipt_id": receipt_key or None,
                    "source": source,
                    "at": _now(),
                    "results": copy.deepcopy(results),
                })

            payload = {
                "ok": applied > 0 or duplicates > 0,
                "plan_id": plan_id,
                "receipt_id": receipt_key or None,
                "duplicate_receipt": False,
                "applied_count": applied,
                "duplicate_count": duplicates,
                "rejected_count": rejected,
                "results": results,
                "plan": plan_view(plan),
                "queue": build_queue_item(plan),
            }
            if receipt_key:
                self._receipts[receipt_cache_key] = copy.deepcopy(payload)
            message = (
                f"已接收 {applied} 个完成分区，重复提交 {duplicates} 个只记一次，"
                f"拒绝 {rejected} 个"
            )
            payload["message"] = message
            return payload, message

    def _request(self, plan: dict[str, Any], request_id: str | None) -> dict[str, Any] | None:
        for request in plan["requests"]:
            if request["request_id"] == request_id:
                return request
        return None

    # -- 断线恢复 ------------------------------------------------------
    def recover(
        self, plan_id: str, *, replay_receipts: list[dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """断线恢复：重建队列并幂等重放回执，交集分区不会被写第二次。"""
        with self._lock:
            plan = self._plans.get(plan_id)
            if plan is None:
                return None, f"分区回填计划 {plan_id} 不存在"

            replays: list[dict[str, Any]] = []
            for receipt in replay_receipts or []:
                if not isinstance(receipt, dict):
                    continue
                result, _message = self.complete_partitions(
                    plan_id,
                    completed_partitions=receipt.get("completed_partitions") or [],
                    receipt_id=receipt.get("receipt_id"),
                    source=str(receipt.get("source") or "batch_merge"),
                )
                if result is not None:
                    replays.append({
                        "receipt_id": result.get("receipt_id"),
                        "duplicate_receipt": result.get("duplicate_receipt", False),
                        "applied_count": result.get("applied_count", 0),
                        "duplicate_count": result.get("duplicate_count", 0),
                    })

            return {
                "plan": plan_view(plan),
                "queue": build_queue_item(plan),
                "completions": [
                    copy.deepcopy(value)
                    for key, value in self._completions.items()
                    if key[0] == plan["table"]
                ],
                "replayed_receipts": replays,
                "receipt_count": len(self._receipts),
            }, f"计划 {plan_id} 已恢复：合并队列重建，重复回执不会重复写入交集分区"

    def reset(self) -> None:
        """测试辅助：清空计划仓库。"""
        with self._lock:
            self._plans.clear()
            self._plan_order.clear()
            self._requests.clear()
            self._request_by_dedup.clear()
            self._completions.clear()
            self._receipts.clear()
            self._plan_sequence = 0
            self._request_sequence = 0


backfill_repository = BackfillPlanRepository()
