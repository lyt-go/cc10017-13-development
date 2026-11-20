"""分区迁移计划：支付表从小时分区切换到日分区的存量兼容方案。

分区迁移计划是湖上分区与Iceberg管理两个模块之间的交接对象：
- 湖上分区负责校验支付表分区定义并生成计划，计划保留旧分区边界与目标分区规格，
  旧小时分区与目标日分区在计划中一一对应；
- 迁移按批次执行，中断后只续做未完成批次，避免缺少映射把支付事件写入错误日期；
- 同一小时分区的旧边界与目标日分区在迁移窗口相遇时，按计划的冲突处置策略
  （先归档旧边界 / 先切换目标元数据）形成不同的计划版本；
- Iceberg管理读取最新计划，核对后写入支付表目标分区元数据，
  并确认每个支付事件只落入一个目标分区。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from app.store import store

PLAN_TABLE = "partition_migration_plans"
SOURCE_GRANULARITY = "hour"
TARGET_GRANULARITY = "day"
MIGRATION_WINDOW_DAYS = 7
HOURS_PER_DAY = 24

# 计划状态：待执行 → 迁移中 →（已中断 / 失败）→ 已完成；被新版本取代后为已替换
PLAN_STATUSES = ["待执行", "迁移中", "已中断", "失败", "已完成", "已替换"]
EXECUTABLE_STATUSES = ["待执行", "迁移中"]
RESUMABLE_STATUSES = ["迁移中", "已中断", "失败"]

# 旧边界与目标日分区在迁移窗口相遇时的两种处置做法，对应不同的计划版本
STRATEGIES = {
    "archive_boundary_first": "先归档旧边界",
    "switch_metadata_first": "先切换目标元数据",
}
STRATEGY_RESOLUTION = {
    "archive_boundary_first": "先归档旧小时分区边界，再切换目标日分区元数据",
    "switch_metadata_first": "先切换目标日分区元数据，再归档旧小时分区边界",
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class PartitionMigrationService:
    """分区迁移计划的生成、执行、中断恢复与落区校验。"""

    # ───────────────────────────────
    # 查询
    # ───────────────────────────────

    def list_plans(self, table_name: str | None = None) -> list[dict[str, Any]]:
        plans = store.rows(PLAN_TABLE)
        if table_name:
            plans = [p for p in plans if p.get("table_name") == table_name]
        return sorted(plans, key=lambda p: (str(p.get("table_name")), -int(p.get("version", 0))))

    def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        for plan in store.rows(PLAN_TABLE):
            if plan.get("plan_id") == plan_id:
                return plan
        return None

    def plans_for_table(self, table_name: str) -> list[dict[str, Any]]:
        return self.list_plans(table_name)

    def latest_plan_for_table(self, table_name: str) -> dict[str, Any] | None:
        """表级最新有效计划（不含已被替换的旧版本）。"""
        candidates = [
            p for p in store.rows(PLAN_TABLE)
            if p.get("table_name") == table_name and p.get("status") != "已替换"
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda p: int(p.get("version", 0)))

    # ───────────────────────────────
    # 生成（迁移计划写入点）
    # ───────────────────────────────

    def generate_plan(
        self,
        entry: dict[str, Any],
        *,
        target_granularity: str = TARGET_GRANULARITY,
        strategy: str = "archive_boundary_first",
    ) -> tuple[dict[str, Any] | None, str]:
        """根据支付表分区定义生成迁移计划：保留旧分区边界与目标分区规格，旧小时分区与目标日分区一一对应。"""
        code = str(entry.get("分区编号", ""))
        table = str(entry.get("目标表名") or "").strip()
        column = str(entry.get("分区列名") or "").strip()
        source_granularity = str(entry.get("分区粒度") or "").strip()
        if not table or not column:
            return None, "分区定义缺少目标表名或分区列名，无法生成迁移计划"
        if source_granularity != SOURCE_GRANULARITY:
            return None, (
                f"分区定义 {code} 当前粒度为「{source_granularity or '未知'}」，"
                f"仅支持从 {SOURCE_GRANULARITY} 迁移到 {TARGET_GRANULARITY}"
            )
        if target_granularity != TARGET_GRANULARITY:
            return None, f"目标粒度「{target_granularity}」不在支持范围，仅支持 {TARGET_GRANULARITY}"
        if strategy not in STRATEGIES:
            return None, f"冲突处置策略「{strategy}」不存在，可选：{'、'.join(STRATEGIES.values())}"

        source_spec = str(entry.get("分区转换") or "").strip() or f"date_trunc('hour', {column})"
        target_spec = f"date_trunc('day', {column})"
        batches = self._build_batches(column)
        plan_id = self._next_plan_id()
        version = self._next_version(table)
        now = _now()
        plan: dict[str, Any] = {
            "plan_id": plan_id,
            "table_name": table,
            "partition_code": code,
            "partition_entry_id": int(entry.get("id", 0)),
            "partition_column": column,
            "source_granularity": SOURCE_GRANULARITY,
            "target_granularity": TARGET_GRANULARITY,
            "source_spec": source_spec,
            "target_spec": target_spec,
            "strategy": strategy,
            "strategy_label": STRATEGIES[strategy],
            "version": version,
            "status": "待执行",
            "batches": batches,
            "mapping_count": sum(len(b["source_partitions"]) for b in batches),
            "total_batches": len(batches),
            "migrated_batches": 0,
            "last_reason": None,
            "assignment_check": None,
            "resume_history": [],
            "created_at": now,
            "updated_at": now,
            "completed_at": None,
            "interrupted_at": None,
            "superseded_by": None,
        }
        # 写入前重点核对：旧小时分区与目标日分区必须一一对应
        errors = self.validate_mappings(plan)
        if errors:
            return None, "；".join(errors)

        # 同一表的旧计划（无论哪种处置做法）被新版本替换
        for old in store.rows(PLAN_TABLE):
            if old.get("table_name") == table and old.get("status") != "已替换":
                old["status"] = "已替换"
                old["superseded_by"] = plan_id
                old["updated_at"] = now
        store.rows(PLAN_TABLE).append(plan)
        return plan, (
            f"分区迁移计划 {plan_id}（v{version}）已生成：{len(batches)} 个批次共 "
            f"{plan['mapping_count']} 个旧小时分区与目标日分区一一对应，冲突处置策略为「{STRATEGIES[strategy]}」"
        )

    # ───────────────────────────────
    # 执行、中断与恢复
    # ───────────────────────────────

    def execute_plan(self, plan_id: str, batch_limit: int | None = 1) -> tuple[dict[str, Any] | None, str]:
        """按批次执行迁移；默认每批一次，可通过 batch_limit 控制。"""
        plan = self.get_plan(plan_id)
        if plan is None:
            return None, f"分区迁移计划 {plan_id} 不存在"
        status = plan.get("status")
        if status == "已中断":
            return None, f"分区迁移计划 {plan_id} 已中断（{plan.get('last_reason') or '未说明原因'}），请恢复迁移"
        if status == "失败":
            return None, f"分区迁移计划 {plan_id} 处于失败状态（{plan.get('last_reason') or '未说明原因'}），请重试"
        if status == "已完成":
            return None, f"分区迁移计划 {plan_id} 已完成，无需重复执行"
        if status == "已替换":
            return None, f"分区迁移计划 {plan_id} 已被 v{plan.get('version', 0) + 1} 及以上版本替换，请执行最新计划"
        ok, reason = self._check_entry_executable(plan)
        if not ok:
            plan["status"] = "失败"
            plan["last_reason"] = reason
            plan["updated_at"] = _now()
            return None, reason
        migrated = self._migrate_batches(plan, batch_limit)
        return self._after_migrate(plan, migrated)

    def interrupt_plan(self, plan_id: str, reason: str | None = None) -> tuple[dict[str, Any] | None, str]:
        """标记中断：记录原因与已迁移范围，迁移中的批次回退为待迁移。"""
        plan = self.get_plan(plan_id)
        if plan is None:
            return None, f"分区迁移计划 {plan_id} 不存在"
        if plan.get("status") not in EXECUTABLE_STATUSES:
            return None, f"仅待执行或迁移中的计划可中断，当前状态「{plan.get('status')}」"
        note = (reason or "").strip() or "人工标记中断"
        for batch in plan["batches"]:
            if batch["status"] == "迁移中":
                batch["status"] = "待迁移"
        plan["status"] = "已中断"
        plan["last_reason"] = note
        plan["interrupted_at"] = _now()
        plan["updated_at"] = _now()
        return plan, (
            f"分区迁移计划 {plan_id} 已中断：{note}；"
            f"已迁移 {plan['migrated_batches']}/{plan['total_batches']} 个批次，恢复后只续做未完成批次"
        )

    def resume_plan(self, plan_id: str, reason: str | None = None) -> tuple[dict[str, Any] | None, str]:
        """中断或失败后恢复：只续做未完成批次，已迁移批次跳过不重复写。"""
        plan = self.get_plan(plan_id)
        if plan is None:
            return None, f"分区迁移计划 {plan_id} 不存在"
        status = plan.get("status")
        if status == "待执行":
            return None, f"分区迁移计划 {plan_id} 尚未开始执行，请直接执行迁移"
        if status == "已完成":
            return None, f"分区迁移计划 {plan_id} 已完成，无需恢复"
        if status == "已替换":
            return None, f"分区迁移计划 {plan_id} 已被新版本替换，请恢复最新计划"
        ok, fail_reason = self._check_entry_executable(plan)
        if not ok:
            plan["status"] = "失败"
            plan["last_reason"] = fail_reason
            plan["updated_at"] = _now()
            return None, fail_reason

        done_before = int(plan.get("migrated_batches", 0))
        note = (reason or "").strip() or str(plan.get("last_reason") or "") or "未说明原因"
        migrated = self._migrate_batches(plan, None)  # 只续做未完成批次
        plan.setdefault("resume_history", []).append({
            "resumed_at": _now(),
            "reason": note,
            "resumed_batches": migrated,
            "skipped_batches": done_before,
        })
        plan, message = self._after_migrate(plan, migrated)
        return plan, f"恢复原因：{note}；续做 {migrated} 个未完成批次，跳过 {done_before} 个已迁移批次。{message}"

    # ───────────────────────────────
    # 校验
    # ───────────────────────────────

    def validate_mappings(self, plan: dict[str, Any]) -> list[str]:
        """核对旧小时分区与目标日分区一一对应：不重复、不遗漏、日期对齐。"""
        errors: list[str] = []
        owner: dict[str, str] = {}
        for batch in plan.get("batches", []):
            target = str(batch.get("target_partition", ""))
            expected_day = target.rsplit("=", 1)[-1]
            for source in batch.get("source_partitions", []):
                day = str(source).split("=", 1)[-1].split("T")[0]
                if day != expected_day:
                    errors.append(f"旧小时分区 {source} 与目标日分区 {target} 不对应")
                if source in owner:
                    errors.append(f"旧小时分区 {source} 在计划中重复映射到 {owner[source]} 与 {target}")
                owner[source] = target
        if len(owner) != int(plan.get("mapping_count", 0)):
            errors.append("旧小时分区映射数量与计划声明不一致，存在遗漏或重复")
        return errors

    def check_event_assignment(self, plan: dict[str, Any]) -> dict[str, Any]:
        """落区校验：迁移后每个支付事件只能落在一个目标分区。"""
        owner: dict[str, str] = {}
        conflicts: list[str] = []
        for batch in plan.get("batches", []):
            target = str(batch.get("target_partition", ""))
            expected_day = target.rsplit("=", 1)[-1]
            for source in batch.get("source_partitions", []):
                day = str(source).split("=", 1)[-1].split("T")[0]
                if day != expected_day:
                    conflicts.append(f"目标日分区 {target} 覆盖了不属于该日期的支付事件 {source}")
                if source in owner and owner[source] != target:
                    conflicts.append(f"支付事件小时分区 {source} 同时落入目标分区 {owner[source]} 与 {target}")
                owner[source] = target
        result = {
            "ok": not conflicts,
            "conflicts": conflicts,
            "checked_sources": len(owner),
            "checked_at": _now(),
        }
        plan["assignment_check"] = result
        return result

    # ───────────────────────────────
    # 内部辅助
    # ───────────────────────────────

    def _build_batches(self, column: str) -> list[dict[str, Any]]:
        """按迁移窗口构造批次：每个目标日分区对应 24 个旧小时分区。"""
        today = date.today()
        batches: list[dict[str, Any]] = []
        for offset in range(MIGRATION_WINDOW_DAYS, 0, -1):
            day = today - timedelta(days=offset)
            sources = [f"{column}={day.isoformat()}T{hour:02d}" for hour in range(HOURS_PER_DAY)]
            batches.append({
                "batch_no": len(batches) + 1,
                "target_partition": f"{column}_day={day.isoformat()}",
                "source_partitions": sources,
                "status": "待迁移",
                "migrated_at": None,
                "resolution": None,
                "error": None,
            })
        return batches

    def _next_plan_id(self) -> str:
        seq = 0
        for plan in store.rows(PLAN_TABLE):
            plan_id = str(plan.get("plan_id", ""))
            if plan_id.startswith("MIGR-"):
                try:
                    seq = max(seq, int(plan_id.split("-", 1)[1]))
                except (IndexError, ValueError):
                    continue
        return f"MIGR-{seq + 1:04d}"

    def _next_version(self, table_name: str) -> int:
        versions = [
            int(p.get("version", 0))
            for p in store.rows(PLAN_TABLE)
            if p.get("table_name") == table_name
        ]
        return max(versions, default=0) + 1

    def _check_entry_executable(self, plan: dict[str, Any]) -> tuple[bool, str]:
        entry = store.find("lake_partition", int(plan.get("partition_entry_id", 0)))
        if entry is None:
            return False, f"分区定义 {plan.get('partition_code')} 不存在或已归档，迁移失败"
        if entry.get("status") == "已停用":
            return False, f"分区定义 {plan.get('partition_code')} 已停用，迁移失败，请恢复定义后重试"
        return True, ""

    def _migrate_batches(self, plan: dict[str, Any], batch_limit: int | None) -> int:
        """迁移未完成批次；旧边界与目标日分区相遇的冲突按计划策略处置。"""
        migrated = 0
        for batch in plan.get("batches", []):
            if batch.get("status") == "已迁移":
                continue
            if batch_limit is not None and migrated >= batch_limit:
                break
            batch["status"] = "迁移中"
            batch["resolution"] = STRATEGY_RESOLUTION[str(plan.get("strategy"))]
            batch["migrated_at"] = _now()
            batch["error"] = None
            batch["status"] = "已迁移"
            migrated += 1
        plan["migrated_batches"] = sum(1 for b in plan.get("batches", []) if b.get("status") == "已迁移")
        plan["updated_at"] = _now()
        return migrated

    def _after_migrate(self, plan: dict[str, Any], migrated: int) -> tuple[dict[str, Any], str]:
        if int(plan.get("migrated_batches", 0)) >= int(plan.get("total_batches", 0)):
            plan["status"] = "已完成"
            plan["completed_at"] = _now()
            plan["last_reason"] = None
            # 全部批次迁移完成后做落区校验，并把分区定义切换到目标规格
            self.check_event_assignment(plan)
            self._apply_target_spec_to_entry(plan)
            return plan, (
                f"分区迁移计划 {plan['plan_id']} 已完成：{plan['mapping_count']} 个旧小时分区"
                f"全部迁移到 {plan['total_batches']} 个目标日分区，分区定义已切换为日粒度"
            )
        plan["status"] = "迁移中"
        return plan, f"已迁移 {migrated} 个批次（进度 {plan['migrated_batches']}/{plan['total_batches']}）"

    def _apply_target_spec_to_entry(self, plan: dict[str, Any]) -> None:
        """迁移完成后把分区定义切换到目标日粒度规格，旧边界保留在计划中供存量查询。"""
        entry = store.find("lake_partition", int(plan.get("partition_entry_id", 0)))
        if entry is None:
            return
        entry["分区粒度"] = plan.get("target_granularity")
        entry["分区转换"] = plan.get("target_spec")
        entry["_migration"] = {
            "plan_id": plan.get("plan_id"),
            "version": plan.get("version"),
            "completed_at": plan.get("completed_at"),
        }


migration_service = PartitionMigrationService()
