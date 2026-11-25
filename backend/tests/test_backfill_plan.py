"""分区回填计划测试：批量安排、交集裁决、幂等接收、合并队列回写。"""
from __future__ import annotations

import sys
sys.path.insert(0, '.')

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.backfill_plan import (
    BackfillPlanError,
    backfill_repository,
    enumerate_partitions,
    normalize_mode,
)

client = TestClient(app)


@pytest.fixture(autouse=True)
def _clean_repository():
    backfill_repository.reset()
    yield
    backfill_repository.reset()


def submit_two_requests(
    table: str = "dwd_order",
    *,
    first_mode: str = "append",
    second_mode: str = "replace",
    first_priority: int | None = None,
    second_priority: int | None = None,
    name: str = "夜间批量回填",
) -> dict:
    def request(label: str, start: str, end: str, mode: str, priority: int | None):
        payload = {
            "label": label,
            "start_partition": start,
            "end_partition": end,
            "mode": mode,
        }
        if priority is not None:
            payload["priority"] = priority
        return payload

    return backfill_repository.submit_plan({
        "name": name,
        "table": table,
        "requests": [
            request("追加区间", "p1", "p3", first_mode, first_priority),
            request("替换区间", "p2", "p4", second_mode, second_priority),
        ],
    })


class TestPartitionParsing:
    def test_numeric_range(self):
        assert enumerate_partitions("p1", "p3") == ["p1", "p2", "p3"]

    def test_inline_range_syntax(self):
        assert enumerate_partitions("p1~p3") == ["p1", "p2", "p3"]
        assert enumerate_partitions("p1 到 p3") == ["p1", "p2", "p3"]

    def test_single_partition(self):
        assert enumerate_partitions("p2") == ["p2"]

    def test_explicit_list(self):
        assert enumerate_partitions("p1,p3") == ["p1", "p3"]

    def test_reverse_range_rejected(self):
        with pytest.raises(BackfillPlanError):
            enumerate_partitions("p4", "p2")

    def test_mode_aliases(self):
        assert normalize_mode("追加") == "append"
        assert normalize_mode("覆盖") == "replace"
        assert normalize_mode("overwrite") == "replace"
        with pytest.raises(BackfillPlanError):
            normalize_mode("merge")


class TestSubmitPlan:
    def test_two_requests_keep_coverage(self):
        result = submit_two_requests()
        plan = result["plan"]
        assert result["created"] is True
        assert sorted(plan["coverage"]) == ["REQ-001", "REQ-002"]
        assert plan["coverage"]["REQ-001"] == ["p1", "p2", "p3"]
        assert plan["coverage"]["REQ-002"] == ["p2", "p3", "p4"]
        assert plan["status"] == "待确认"
        modes = {request["request_id"]: request["mode"] for request in plan["requests"]}
        assert modes == {"REQ-001": "append", "REQ-002": "replace"}

    def test_intersection_conflict_resolved_by_priority(self):
        result = submit_two_requests()
        queue = result["queue"]
        assert queue["intersection"]["partitions"] == ["p2", "p3"]
        assert queue["intersection"]["status"] == "已裁决"
        for disposition in queue["intersection"]["dispositions"]:
            assert disposition["resolved"] is True
            assert disposition["candidate_modes"] == ["append", "replace"]
            assert disposition["winner_request"] == "REQ-002"
            assert disposition["mode"] == "replace"
            assert len(disposition["candidates"]) == 2

    def test_non_intersection_scope(self):
        queue = submit_two_requests()["queue"]
        assert queue["non_intersection_scope"] == {"REQ-001": ["p1"], "REQ-002": ["p4"]}
        assert queue["intersection_scope"] == {"REQ-001": [], "REQ-002": ["p2", "p3"]}
        assert len(queue["original_requests"]) == 2
        assert queue["committable_scope"] == queue["non_intersection_scope"]

    def test_duplicate_submission_recorded_once(self):
        first = submit_two_requests()
        second = submit_two_requests(name="换个名字")
        assert first["created"] is True
        assert second["created"] is False
        assert second["deduplicated"] is True
        assert second["plan"]["plan_id"] == first["plan"]["plan_id"]
        assert second["duplicate_submissions"] == 2
        assert backfill_repository.list_plans()["total"] == 1

    def test_same_partition_name_isolated_by_table(self):
        first = submit_two_requests(table="dwd_order")
        second = submit_two_requests(table="dwd_user")
        assert first["plan"]["plan_id"] != second["plan"]["plan_id"]
        assert first["queue"]["table"] == "dwd_order"
        assert second["queue"]["table"] == "dwd_user"

    def test_missing_request_rejected(self):
        with pytest.raises(BackfillPlanError):
            backfill_repository.submit_plan({"table": "dwd_order", "requests": []})

    def test_per_partition_modes(self):
        result = backfill_repository.submit_plan({
            "table": "dwd_order",
            "requests": [
                {
                    "label": "范围内逐分区配置",
                    "start_partition": "p1",
                    "end_partition": "p1",
                    "mode": "append",
                    "priority": 6,
                    "partition_modes": {"p1": "replace"},
                },
                {
                    "label": "另一个",
                    "start_partition": "p1",
                    "end_partition": "p1",
                    "mode": "append",
                    "priority": 5,
                },
            ],
        })
        disposition = result["queue"]["intersection"]["dispositions"][0]
        assert disposition["resolved"] is True
        assert disposition["winner_request"] == "REQ-001"
        assert disposition["mode"] == "replace"


class TestConfirm:
    def test_confirm_refreshes_queue(self):
        result = submit_two_requests()
        plan_id = result["plan"]["plan_id"]
        confirmed = backfill_repository.confirm_plan(plan_id)[0]
        assert confirmed["plan"]["status"] == "已确认"
        assert confirmed["queue"]["confirmed"] is True
        assert confirmed["queue"]["status"] == "待提交"

    def test_concurrent_confirm_is_idempotent(self):
        plan_id = submit_two_requests()["plan"]["plan_id"]
        first = backfill_repository.confirm_plan(plan_id)
        second = backfill_repository.confirm_plan(plan_id)
        assert first[2] is False
        assert second[2] is True
        assert first[0]["plan"]["confirmed_at"] == second[0]["plan"]["confirmed_at"]
        assert len(first[0]["plan"]["receipts"]) == len(second[0]["plan"]["receipts"]) == 0

    def test_manual_conflict_blocks_confirm_until_resolved(self):
        plan_id = submit_two_requests(first_priority=5, second_priority=5)["plan"]["plan_id"]
        result, message, _already = backfill_repository.confirm_plan(plan_id)
        assert result["queue"]["intersection"]["status"] == "待裁决"
        assert "仍需裁决" in message
        assert result["plan"]["status"] == "待确认"

        backfill_repository.resolve_intersection(plan_id, winner_request="REQ-001")
        confirmed, message, _already = backfill_repository.confirm_plan(plan_id)
        assert confirmed["plan"]["status"] == "已确认"
        assert "已确认" in message
        dispositions = confirmed["queue"]["intersection"]["dispositions"]
        assert all(item["winner_request"] == "REQ-001" for item in dispositions)
        assert all(item["rule"] == "manual" for item in dispositions)


class TestCompletions:
    def test_receive_writes_to_owning_request(self):
        result = submit_two_requests()
        plan_id = result["plan"]["plan_id"]
        backfill_repository.confirm_plan(plan_id)
        outcome, message = backfill_repository.complete_partitions(
            plan_id, completed_partitions=["p1", "p2"], receipt_id="BM-1")
        written = {item["partition"]: item for item in outcome["results"]}
        assert written["p1"]["written_to"] == "REQ-001"
        assert written["p1"]["mode"] == "append"
        assert written["p2"]["written_to"] == "REQ-002"
        assert written["p2"]["mode"] == "replace"
        assert written["p2"]["kind"] == "交集"
        assert outcome["applied_count"] == 2
        assert outcome["plan"]["received_partitions"] == ["p1", "p2"]
        assert outcome["plan"]["status"] == "部分完成"
        assert outcome["queue"]["status"] == "部分接收"

    def test_duplicate_partition_only_recorded_once(self):
        plan_id = submit_two_requests()["plan"]["plan_id"]
        backfill_repository.confirm_plan(plan_id)
        backfill_repository.complete_partitions(
            plan_id, completed_partitions=["p2", "p2"], receipt_id="BM-1")
        outcome, _message = backfill_repository.complete_partitions(
            plan_id, completed_partitions=["p2", "p3"], receipt_id="BM-2")
        assert outcome["applied_count"] == 1
        assert outcome["duplicate_count"] == 1
        duplicate = next(item for item in outcome["results"] if item["duplicate"])
        assert duplicate["partition"] == "p2"
        received = outcome["plan"]["received_partitions"]
        assert received.count("p2") == 1

    def test_same_receipt_replay_is_idempotent(self):
        plan_id = submit_two_requests()["plan"]["plan_id"]
        backfill_repository.confirm_plan(plan_id)
        first, _ = backfill_repository.complete_partitions(
            plan_id, completed_partitions=["p2", "p3"], receipt_id="BM-1")
        replay, _ = backfill_repository.complete_partitions(
            plan_id, completed_partitions=["p2", "p3"], receipt_id="BM-1")
        assert first["applied_count"] == 2
        assert replay["duplicate_receipt"] is True
        assert replay["applied_count"] == 0
        assert replay["plan"]["received_partitions"] == ["p2", "p3"]

    def test_out_of_plan_partition_rejected(self):
        plan_id = submit_two_requests()["plan"]["plan_id"]
        backfill_repository.confirm_plan(plan_id)
        outcome, _ = backfill_repository.complete_partitions(
            plan_id, completed_partitions=["p9"], receipt_id="BM-1")
        assert outcome["rejected_count"] == 1
        assert outcome["plan"]["received_partitions"] == []

    def test_unconfirmed_plan_rejects_completions(self):
        plan_id = submit_two_requests()["plan"]["plan_id"]
        result, message = backfill_repository.complete_partitions(
            plan_id, completed_partitions=["p1"], receipt_id="BM-1")
        assert result is None
        assert "尚未确认" in message

    def test_intersection_lookback_checks_both_sources(self):
        plan_id = submit_two_requests()["plan"]["plan_id"]
        backfill_repository.confirm_plan(plan_id)
        outcome, _ = backfill_repository.complete_partitions(
            plan_id, completed_partitions=["p3"], receipt_id="BM-1")
        lookback = outcome["results"][0]["source_lookback"]
        assert {item["request_id"] for item in lookback} == {"REQ-001", "REQ-002"}
        assert all(item["found"] for item in lookback)

    def test_lookback_failure_rejects_intersection(self):
        plan_id = submit_two_requests()["plan"]["plan_id"]
        backfill_repository.confirm_plan(plan_id)
        outcome, _ = backfill_repository.complete_partitions(
            plan_id,
            completed_partitions=["p3"],
            receipt_id="BM-1",
            sources={"p3": [{"source": "原请求 REQ-002", "request_id": "REQ-002", "found": False}]},
        )
        assert outcome["applied_count"] == 0
        assert outcome["rejected_count"] == 1
        assert "双来源" in outcome["results"][0]["reason"]

    def test_same_partition_isolated_across_tables(self):
        first = submit_two_requests(table="dwd_order")
        second = submit_two_requests(table="dwd_user")
        backfill_repository.confirm_plan(first["plan"]["plan_id"])
        backfill_repository.confirm_plan(second["plan"]["plan_id"])
        backfill_repository.complete_partitions(
            first["plan"]["plan_id"], completed_partitions=["p1"], receipt_id="A-1")
        second_outcome, _ = backfill_repository.complete_partitions(
            second["plan"]["plan_id"], completed_partitions=["p1"], receipt_id="B-1")
        assert second_outcome["applied_count"] == 1
        assert second_outcome["duplicate_count"] == 0

    def test_full_completion_marks_plan_done(self):
        plan_id = submit_two_requests()["plan"]["plan_id"]
        backfill_repository.confirm_plan(plan_id)
        outcome, _ = backfill_repository.complete_partitions(
            plan_id, completed_partitions=["p1", "p2", "p3", "p4"], receipt_id="BM-1")
        assert outcome["plan"]["status"] == "已完成"
        assert outcome["queue"]["status"] == "已完成"
        assert outcome["plan"]["pending_partitions"] == []

    def test_recovery_replays_receipts_without_double_write(self):
        plan_id = submit_two_requests()["plan"]["plan_id"]
        backfill_repository.confirm_plan(plan_id)
        backfill_repository.complete_partitions(
            plan_id, completed_partitions=["p2", "p3"], receipt_id="BM-1")
        recovered, message = backfill_repository.recover(
            plan_id,
            replay_receipts=[{
                "receipt_id": "BM-1",
                "completed_partitions": ["p2", "p3"],
            }],
        )
        assert "已恢复" in message
        assert recovered["replayed_receipts"][0]["duplicate_receipt"] is True
        assert recovered["replayed_receipts"][0]["applied_count"] == 0
        assert recovered["plan"]["received_partitions"] == ["p2", "p3"]


class TestApi:
    def test_full_flow_endpoint(self):
        submit = client.post("/api/backfill-plans", json={
            "name": "夜间批量回填",
            "table": "dwd_order",
            "requests": [
                {"label": "追加", "start_partition": "p1", "end_partition": "p3", "mode": "append"},
                {"label": "替换", "start_partition": "p2", "end_partition": "p4", "mode": "replace"},
            ],
        })
        assert submit.status_code == 200
        body = submit.json()
        plan_id = body["plan"]["plan_id"]

        listed = client.get("/api/backfill-plans").json()
        assert listed["total"] == 1
        assert listed["items"][0]["pending_partitions"] == ["p1", "p2", "p3", "p4"]
        assert listed["items"][0]["coverage"]["REQ-001"] == ["p1", "p2", "p3"]

        queue = client.get("/api/backfill-plans/merge-queue").json()
        assert queue["items"][0]["intersection"]["dispositions"][0]["resolved"] is True
        assert len(queue["items"][0]["original_requests"]) == 2

        confirm = client.post(f"/api/backfill-plans/{plan_id}/confirm", json={})
        assert confirm.status_code == 200
        assert confirm.json()["ok"] is True

        completions = client.post(f"/api/backfill-plans/{plan_id}/completions", json={
            "completed_partitions": ["p1", "p2", "p3"],
            "receipt_id": "BM-1",
        })
        assert completions.status_code == 200
        payload = completions.json()
        assert payload["applied_count"] == 3
        assert payload["plan"]["received_partitions"] == ["p1", "p2", "p3"]
        assert payload["plan"]["pending_partitions"] == ["p4"]
        assert payload["queue"]["next_committable_partitions"] == ["p4"]

        detail = client.get(f"/api/backfill-plans/{plan_id}").json()
        assert detail["queue"]["plan_id"] == plan_id
        assert detail["plan"]["intersection_partitions"] == ["p2", "p3"]

    def test_unknown_plan_returns_404(self):
        assert client.get("/api/backfill-plans/PLAN-404").status_code == 404
        assert client.post("/api/backfill-plans/PLAN-404/confirm", json={}).status_code == 404

    def test_invalid_mode_returns_400(self):
        response = client.post("/api/backfill-plans", json={
            "table": "dwd_order",
            "requests": [{"start_partition": "p1", "mode": "merge"}],
        })
        assert response.status_code == 400

    def test_batch_merge_service_receives_completions(self):
        from app.services import service_for

        submit = submit_two_requests()
        plan_id = submit["plan"]["plan_id"]
        backfill_repository.confirm_plan(plan_id)
        service = service_for("batch_merge")
        result = service.domain_receive_backfill_plan(
            1, plan_id, ["p2"], receipt_id="SVC-1")
        assert result["ok"] is True
        assert result["applied_count"] == 1
        assert result["results"][0]["written_to"] == "REQ-002"
        assert result["writeback_queue"]["received_partitions"] == ["p2"]


class TestConcurrency:
    def test_parallel_confirm_and_receipts_never_double_write(self):
        import threading

        plan_id = submit_two_requests()["plan"]["plan_id"]
        barrier = threading.Barrier(6)
        confirmations: list[bool] = []
        lock = threading.Lock()

        def confirm():
            barrier.wait()
            _result, _message, already = backfill_repository.confirm_plan(plan_id)
            with lock:
                confirmations.append(already)

        threads = [threading.Thread(target=confirm) for _ in range(3)]
        for thread in threads:
            thread.start()

        def complete(receipt: str):
            barrier.wait()
            backfill_repository.complete_partitions(
                plan_id, completed_partitions=["p2", "p3"], receipt_id=receipt)

        for index in range(3):
            threads.append(threading.Thread(target=complete, args=(f"RACE-{index}",)))
        for thread in threads[3:]:
            thread.start()
        for thread in threads:
            thread.join()

        # 只有第一次确认真正落定，其余都是幂等确认
        assert confirmations.count(False) == 1
        assert confirmations.count(True) == 2

        detail = backfill_repository.plan_detail(plan_id)["plan"]
        assert detail["received_partitions"] == ["p2", "p3"]
        assert detail["status"] == "部分完成"
        # p2、p3 是交集分区，三条回执并发也只各写一次
        assert len(detail["receipts"]) == 0
        received = [
            item for item in detail["partitions"] if item["status"] == "已接收"
        ]
        assert len(received) == 2
        assert all(
            len(item["adjudication"]["source_lookback"]) == 2 for item in received
        )

    def test_same_receipt_id_across_tables_does_not_collide(self):
        first = submit_two_requests(table="dwd_order")
        second = submit_two_requests(table="dwd_user")
        for submitted in (first, second):
            backfill_repository.confirm_plan(submitted["plan"]["plan_id"])
        backfill_repository.complete_partitions(
            first["plan"]["plan_id"], completed_partitions=["p1"], receipt_id="SAME")
        outcome, _ = backfill_repository.complete_partitions(
            second["plan"]["plan_id"], completed_partitions=["p1"], receipt_id="SAME")
        assert outcome["applied_count"] == 1
        assert outcome["duplicate_receipt"] is False
