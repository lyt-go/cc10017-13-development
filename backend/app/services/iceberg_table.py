"""Iceberg管理业务规则：状态流转、字段校验与筛选口径都收在这里。

增强：读取湖上分区生成的分区迁移计划，核对后写入支付表目标分区元数据，
并确认迁移后每个支付事件只落入一个目标分区。
"""
from __future__ import annotations

from typing import Any

from app.services.partition_migration import migration_service
from app.store import store

MODULE = "iceberg_table"
LIST_FIELDS = ["表编号", "表名称", "存储格式", "分区策略", "排序字段", "快照过期", "孤立文件清理", "元数据版本"]
REQUIRED_FIELDS = ["表编号", "表名称", "存储格式"]
STATUS_ORDER = ["草稿", "待校验", "已就绪", "已下线"]
ACTION_RULES = {"提交校验": "待校验", "确认就绪": "已就绪", "标记下线": "已下线"}
NEGATIVE_ACTIONS = []


def _bump_metadata_version(current: Any) -> str:
    digits = "".join(ch for ch in str(current or "") if ch.isdigit())
    return f"v{int(digits) + 1}" if digits else "v2"


class IcebergTableService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("表编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"表元数据 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于Iceberg管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"表元数据已{action}"

    # ───────────────────────────────
    # 分区迁移计划：读取交接对象并写入目标分区元数据
    # ───────────────────────────────

    def list_migration_plans(self, entry_id: int) -> list[dict[str, Any]] | None:
        """表关联的分区迁移计划；表不存在时返回 None。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        return migration_service.plans_for_table(str(entry.get("表名称", "")))

    def verify_target_metadata(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """核对分区迁移计划并写入支付表目标分区元数据。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"表元数据 {entry_id} 不存在或已归档"
        table = str(entry.get("表名称", ""))
        plan = migration_service.latest_plan_for_table(table)
        if plan is None:
            return None, f"表 {table} 未找到分区迁移计划，请先在湖上分区生成"
        if plan.get("status") != "已完成":
            remaining = int(plan.get("total_batches", 0)) - int(plan.get("migrated_batches", 0))
            return None, (
                f"迁移计划 {plan.get('plan_id')} 状态为「{plan.get('status')}」，"
                f"剩余 {remaining} 个批次未完成，无法核对目标元数据"
            )
        # 落区校验：中断恢复后确认没有支付事件同时落在两个目标分区
        check = migration_service.check_event_assignment(plan)
        if not check["ok"]:
            return None, "存在支付事件同时落入两个目标分区：" + "；".join(check["conflicts"][:3])

        new_version = _bump_metadata_version(entry.get("元数据版本"))
        entry["元数据版本"] = new_version
        entry["目标分区规格"] = plan.get("target_spec")
        verify = {
            "plan_id": plan.get("plan_id"),
            "plan_version": plan.get("version"),
            "target_spec": plan.get("target_spec"),
            "metadata_version": new_version,
            "assignment_ok": True,
            "checked_sources": check["checked_sources"],
            "verified_at": check["checked_at"],
        }
        entry["_migration_verify"] = verify
        store.rows("iceberg_partition_specs").append({
            "table_name": table,
            "plan_id": plan.get("plan_id"),
            "plan_version": plan.get("version"),
            "partition_column": plan.get("partition_column"),
            "granularity": plan.get("target_granularity"),
            "source_spec": plan.get("source_spec"),
            "target_spec": plan.get("target_spec"),
            "strategy": plan.get("strategy"),
            "metadata_version": new_version,
            "assignment_ok": True,
            "checked_sources": check["checked_sources"],
            "verified_at": check["checked_at"],
            "source": "partition_migration_plan",
        })
        return entry, (
            f"已按迁移计划 {plan.get('plan_id')}（v{plan.get('version')}）写入目标分区元数据："
            f"元数据版本升至 {new_version}，{check['checked_sources']} 个支付事件小时分区校验通过，"
            "每个支付事件仅落入一个目标分区"
        )
