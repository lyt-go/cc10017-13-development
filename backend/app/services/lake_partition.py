"""湖上分区业务规则：状态流转、字段校验与筛选口径都收在这里。

增强：新增支付表分区定义校验（粒度、分区转换与迁移前置），并生成
小时 → 日粒度的分区迁移计划，迁移计划是交给Iceberg管理的交接对象。
"""
from __future__ import annotations

from typing import Any

from app.services.partition_migration import migration_service
from app.store import store

MODULE = "lake_partition"
LIST_FIELDS = ["分区编号", "目标表名", "分区列名", "分区粒度", "分区转换", "存储位置", "文件格式", "生命周期"]
REQUIRED_FIELDS = ["分区编号", "目标表名", "分区列名"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用分区": "已停用"}
NEGATIVE_ACTIONS = ["停用分区"]
VALID_GRANULARITIES = ["hour", "day", "month"]
PAYMENT_TABLE = "payments"


class LakePartitionService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("分区编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def validate_definition(self, values: dict[str, Any]) -> list[str]:
        """校验分区定义：粒度合法、分区转换引用分区列名、日粒度切换具备已完成的迁移计划。"""
        errors: list[str] = []
        table = str(values.get("目标表名") or "").strip()
        column = str(values.get("分区列名") or "").strip()
        granularity = str(values.get("分区粒度") or "").strip()
        transform = str(values.get("分区转换") or "").strip()
        if granularity and granularity not in VALID_GRANULARITIES:
            errors.append(f"分区粒度「{granularity}」不合法，可选：{'、'.join(VALID_GRANULARITIES)}")
        if column and transform and column not in transform:
            errors.append(f"分区转换「{transform}」未引用分区列名 {column}")
        if table and granularity == "day":
            entry_id = int(values.get("id", 0) or 0)
            hourly_others = [
                row for row in store.rows(MODULE)
                if str(row.get("目标表名")) == table
                and str(row.get("分区粒度")) == "hour"
                and row.get("status") != "已停用"
                and int(row.get("id", 0)) != entry_id
            ]
            if hourly_others:
                plan = migration_service.latest_plan_for_table(table)
                if plan is None or plan.get("status") != "已完成":
                    errors.append(
                        f"表 {table} 存在小时粒度分区定义，切换到日粒度前需要已完成的分区迁移计划，"
                        "否则存量小时分区没有到日分区的迁移关系"
                    )
        return errors

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, [f"缺少必填字段：{'、'.join(missing)}"]
        errors = self.validate_definition(values)
        if errors:
            return None, errors
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
            return None, f"分区定义 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于湖上分区可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "提交校验":
            errors = self.validate_definition(entry)
            if errors:
                return None, "；".join(errors)
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"分区定义已{action}"

    # ───────────────────────────────
    # 分区迁移计划（交接对象由 migration_service 统一管理）
    # ───────────────────────────────

    def generate_migration_plan(
        self, entry_id: int, strategy: str = "archive_boundary_first"
    ) -> tuple[dict[str, Any] | None, str]:
        """校验支付表分区定义并生成小时 → 日粒度的分区迁移计划。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"分区定义 {entry_id} 不存在或已归档"
        return migration_service.generate_plan(entry, strategy=strategy)
