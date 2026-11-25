"""跨模块业务链路测试：验证5条chain端点的请求结构和响应。"""
from __future__ import annotations

import sys
sys.path.insert(0, '.')

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


class TestChainStreamIngest:
    def test_chain_ok(self):
        resp = client.post(
            "/api/chains/stream-ingest",
            params={"source_id": 1, "ingest_id": 1, "commit_id": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert len(data["steps"]) == 4
        assert data["chain"] == "stream-ingest"

    def test_chain_invalid_source(self):
        resp = client.post(
            "/api/chains/stream-ingest",
            params={"source_id": 99999, "ingest_id": 1, "commit_id": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is False


class TestChainStorageOptimize:
    def test_chain_ok(self):
        resp = client.post(
            "/api/chains/storage-optimize",
            params={"table_id": 1, "merge_id": 1, "compaction_id": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert len(data["steps"]) == 4
        assert data["chain"] == "storage-optimize"


class TestChainSchemaEvolve:
    def test_chain_ok(self):
        resp = client.post(
            "/api/chains/schema-evolve",
            params={"schema_id": 1, "backfill_id": 1, "merge_id": 1, "checkpoint_id": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert len(data["steps"]) == 4
        assert data["chain"] == "schema-evolve"


class TestChainLifecycle:
    def test_chain_ok(self):
        resp = client.post(
            "/api/chains/lifecycle",
            params={"snapshot_id": 1, "clean_id": 1, "retention_id": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert len(data["steps"]) == 3
        assert data["chain"] == "lifecycle"


class TestChainMetadataGovern:
    def test_chain_ok(self):
        resp = client.post(
            "/api/chains/metadata-govern",
            params={"lineage_id": 1, "audit_id": 1, "catalog_id": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert len(data["steps"]) == 3
        assert data["chain"] == "metadata-govern"


class TestHealthAndOverview:
    def test_health(self):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_overview(self):
        resp = client.get("/api/overview")
        assert resp.status_code == 200
        data = resp.json()
        assert "cards" in data
        assert "modules" in data

class TestChainBackfillBatch:
    def test_chain_arrange_and_receive(self):
        resp = client.post(
            "/api/chains/backfill-batch",
            json={
                "table": "dwd_order",
                "requests": [
                    {"label": "追加区间", "start_partition": "p1", "end_partition": "p3", "mode": "append"},
                    {"label": "替换区间", "start_partition": "p2", "end_partition": "p4", "mode": "replace"},
                ],
                "completed_partitions": ["p1", "p2", "p3"],
                "receipt_id": "CHAIN-1",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert [step["step"] for step in data["steps"]] == [
            "arrange_backfill", "confirm_plan", "merge_queue", "batch_merge_receive",
        ]
        assert data["steps"][3]["details"]["applied_count"] == 3

    def test_chain_requires_input(self):
        resp = client.post("/api/chains/backfill-batch", json={"table": "dwd_order"})
        assert resp.status_code == 400
