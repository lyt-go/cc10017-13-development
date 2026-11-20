"""分区迁移计划链路测试：支付表小时分区 → 日分区的存量兼容迁移。

覆盖：支付表分区定义校验、迁移计划生成（旧边界与目标规格一一对应）、
中断后只续做未完成批次、冲突处置策略形成不同版本、Iceberg核对目标元数据。
"""
from __future__ import annotations

import unittest

from app.pipeline_engine import pipeline_engine
from app.services.iceberg_table import IcebergTableService
from app.services.lake_partition import LakePartitionService
from app.services.partition_migration import migration_service
from app.store import store


def _create_hourly_payment_partition(svc: LakePartitionService, code: str, table: str = "payments"):
    entry, errors = svc.create_entry({
        "分区编号": code,
        "目标表名": table,
        "分区列名": "pay_time",
        "分区粒度": "hour",
        "分区转换": "date_trunc('hour', pay_time)",
        "存储位置": f"s3://lake/{table}",
        "文件格式": "Parquet",
        "生命周期": "7d",
    })
    assert entry is not None, errors
    return entry


class TestPaymentPartitionValidation(unittest.TestCase):
    """支付表分区定义校验"""

    def setUp(self) -> None:
        self.svc = LakePartitionService()

    def test_day_granularity_requires_completed_plan(self) -> None:
        """存在小时粒度定义时，日粒度定义必须依附已完成的迁移计划。"""
        hourly, errors = self.svc.create_entry({
            "分区编号": "LAKE-V001", "目标表名": "pay_validate", "分区列名": "pay_time",
            "分区粒度": "hour", "分区转换": "date_trunc('hour', pay_time)",
        })
        self.assertIsNotNone(hourly, errors)
        entry, errors = self.svc.create_entry({
            "分区编号": "LAKE-V002", "目标表名": "pay_validate", "分区列名": "pay_time",
            "分区粒度": "day", "分区转换": "date_trunc('day', pay_time)",
        })
        self.assertIsNone(entry)
        self.assertTrue(any("迁移计划" in e for e in errors))

    def test_invalid_granularity_rejected(self) -> None:
        """非法粒度值被拦下并说明原因。"""
        entry, errors = self.svc.create_entry({
            "分区编号": "LAKE-V003", "目标表名": "pay_validate", "分区列名": "pay_time",
            "分区粒度": "minute", "分区转换": "date_trunc('minute', pay_time)",
        })
        self.assertIsNone(entry)
        self.assertTrue(any("分区粒度" in e for e in errors))

    def test_transform_must_reference_column(self) -> None:
        """分区转换必须引用分区列名。"""
        entry, errors = self.svc.create_entry({
            "分区编号": "LAKE-V004", "目标表名": "pay_validate", "分区列名": "pay_time",
            "分区粒度": "hour", "分区转换": "date_trunc('hour', created_at)",
        })
        self.assertIsNone(entry)
        self.assertTrue(any("分区列名" in e for e in errors))


class TestMigrationPlanGeneration(unittest.TestCase):
    """迁移计划生成：保留旧分区边界与目标分区规格，一一对应"""

    def setUp(self) -> None:
        self.svc = LakePartitionService()

    def test_plan_keeps_boundaries_and_one_to_one_mapping(self) -> None:
        entry = _create_hourly_payment_partition(self.svc, "LAKE-G001")
        plan, message = self.svc.generate_migration_plan(entry["id"], "archive_boundary_first")
        self.assertIsNotNone(plan, message)
        # 保留旧分区边界与目标分区规格
        self.assertEqual(plan["source_spec"], "date_trunc('hour', pay_time)")
        self.assertEqual(plan["target_spec"], "date_trunc('day', pay_time)")
        self.assertEqual(plan["source_granularity"], "hour")
        self.assertEqual(plan["target_granularity"], "day")
        # 旧小时分区与目标日分区一一对应
        sources = [s for b in plan["batches"] for s in b["source_partitions"]]
        self.assertEqual(len(sources), len(set(sources)))
        self.assertEqual(plan["mapping_count"], 7 * 24)
        self.assertEqual(plan["total_batches"], 7)
        for batch in plan["batches"]:
            day = batch["target_partition"].rsplit("=", 1)[-1]
            self.assertEqual(len(batch["source_partitions"]), 24)
            for source in batch["source_partitions"]:
                self.assertTrue(source.split("=", 1)[1].startswith(day))
        self.assertEqual(migration_service.validate_mappings(plan), [])

    def test_generate_requires_hour_source(self) -> None:
        """非小时粒度的定义不能生成小时→日迁移计划。"""
        entry, errors = self.svc.create_entry({
            "分区编号": "LAKE-G002", "目标表名": "pay_validate", "分区列名": "pay_time",
            "分区粒度": "month", "分区转换": "date_trunc('month', pay_time)",
        })
        self.assertIsNotNone(entry, errors)
        plan, message = self.svc.generate_migration_plan(entry["id"], "archive_boundary_first")
        self.assertIsNone(plan)
        self.assertIn("hour", message)

    def test_conflict_strategies_form_different_versions(self) -> None:
        """先归档旧边界与先切换目标元数据两种做法形成不同计划版本。"""
        entry = _create_hourly_payment_partition(self.svc, "LAKE-G003")
        plan_v1, _ = self.svc.generate_migration_plan(entry["id"], "archive_boundary_first")
        plan_v2, _ = self.svc.generate_migration_plan(entry["id"], "switch_metadata_first")
        self.assertIsNotNone(plan_v1)
        self.assertIsNotNone(plan_v2)
        self.assertEqual(plan_v2["version"], plan_v1["version"] + 1)
        self.assertEqual(plan_v1["strategy"], "archive_boundary_first")
        self.assertEqual(plan_v2["strategy"], "switch_metadata_first")
        # 旧版本被替换，最新有效计划是 v2
        self.assertEqual(plan_v1["status"], "已替换")
        self.assertEqual(plan_v1["superseded_by"], plan_v2["plan_id"])
        latest = migration_service.latest_plan_for_table("payments")
        self.assertEqual(latest["plan_id"], plan_v2["plan_id"])


class TestMigrationExecution(unittest.TestCase):
    """迁移执行、中断与恢复"""

    def setUp(self) -> None:
        self.svc = LakePartitionService()

    def test_resume_only_processes_unfinished_batches(self) -> None:
        """中断后恢复只续做未完成批次，已迁移批次不重复执行。"""
        entry = _create_hourly_payment_partition(self.svc, "LAKE-E001")
        plan, _ = self.svc.generate_migration_plan(entry["id"], "archive_boundary_first")
        plan_id = plan["plan_id"]

        plan, message = migration_service.execute_plan(plan_id, batch_limit=2)
        self.assertEqual(plan["status"], "迁移中")
        self.assertEqual(plan["migrated_batches"], 2)
        migrated_marks = [b["migrated_at"] for b in plan["batches"][:2]]

        plan, message = migration_service.interrupt_plan(plan_id, "模拟中断：迁移窗口人工暂停")
        self.assertEqual(plan["status"], "已中断")
        self.assertIn("模拟中断", plan["last_reason"])

        plan, message = migration_service.resume_plan(plan_id)
        self.assertEqual(plan["status"], "已完成")
        self.assertIn("续做 5 个未完成批次", message)
        self.assertIn("跳过 2 个已迁移批次", message)
        # 已迁移批次的时间戳未被改写：没有重复迁移
        self.assertEqual([b["migrated_at"] for b in plan["batches"][:2]], migrated_marks)
        self.assertTrue(all(b["status"] == "已迁移" for b in plan["batches"]))
        # 完成后分区定义切换为日粒度，落区校验通过
        self.assertEqual(store.find("lake_partition", entry["id"])["分区粒度"], "day")
        self.assertTrue(plan["assignment_check"]["ok"])

    def test_execute_fails_when_partition_disabled(self) -> None:
        """分区定义被停用后执行失败并说明原因，恢复定义后可重试。"""
        entry = _create_hourly_payment_partition(self.svc, "LAKE-E002")
        plan, _ = self.svc.generate_migration_plan(entry["id"], "archive_boundary_first")
        plan_id = plan["plan_id"]

        self.svc.run_action(entry["id"], "停用分区")
        result, message = migration_service.execute_plan(plan_id)
        self.assertIsNone(result)
        self.assertIn("已停用", message)
        self.assertEqual(plan["status"], "失败")
        self.assertIn("已停用", plan["last_reason"])

        # 恢复定义后重试：续做未完成批次直至完成
        self.svc.run_action(entry["id"], "提交校验")
        self.svc.run_action(entry["id"], "标记生效")
        plan, message = migration_service.resume_plan(plan_id, "停用恢复后重试")
        self.assertEqual(plan["status"], "已完成")
        self.assertIn("停用恢复后重试", message)

    def test_interrupt_requires_active_plan(self) -> None:
        """已完成的计划不能再中断。"""
        entry = _create_hourly_payment_partition(self.svc, "LAKE-E003")
        plan, _ = self.svc.generate_migration_plan(entry["id"], "archive_boundary_first")
        plan_id = plan["plan_id"]
        migration_service.execute_plan(plan_id, batch_limit=7)
        result, message = migration_service.interrupt_plan(plan_id, "迟到的中断")
        self.assertIsNone(result)
        self.assertIn("已完成", message)


class TestIcebergVerifyTargetMetadata(unittest.TestCase):
    """Iceberg管理读取迁移计划并写入支付表目标分区元数据"""

    def setUp(self) -> None:
        self.lake_svc = LakePartitionService()
        self.iceberg_svc = IcebergTableService()

    def _completed_plan(self, code: str):
        entry = _create_hourly_payment_partition(self.lake_svc, code)
        plan, _ = self.lake_svc.generate_migration_plan(entry["id"], "archive_boundary_first")
        migration_service.execute_plan(plan["plan_id"], batch_limit=7)
        return plan

    def test_verify_rejects_incomplete_plan(self) -> None:
        """计划未完成时核对失败并说明剩余批次。"""
        entry = _create_hourly_payment_partition(self.lake_svc, "LAKE-I001")
        plan, _ = self.lake_svc.generate_migration_plan(entry["id"], "archive_boundary_first")
        migration_service.execute_plan(plan["plan_id"], batch_limit=1)
        result, message = self.iceberg_svc.verify_target_metadata(3)
        self.assertIsNone(result)
        self.assertIn("剩余", message)

    def test_verify_writes_target_metadata(self) -> None:
        """核对通过后写入目标分区规格并提升元数据版本。"""
        plan = self._completed_plan("LAKE-I002")
        iceberg = store.find("iceberg_table", 3)
        before_version = iceberg["元数据版本"]

        result, message = self.iceberg_svc.verify_target_metadata(3)
        self.assertIsNotNone(result, message)
        self.assertNotEqual(result["元数据版本"], before_version)
        self.assertEqual(result["目标分区规格"], plan["target_spec"])
        verify = result["_migration_verify"]
        self.assertEqual(verify["plan_id"], plan["plan_id"])
        self.assertTrue(verify["assignment_ok"])
        self.assertEqual(verify["checked_sources"], 7 * 24)
        # 目标分区元数据产物已写入
        specs = store.rows("iceberg_partition_specs")
        self.assertTrue(any(
            s.get("plan_id") == plan["plan_id"] and s.get("source") == "partition_migration_plan"
            for s in specs
        ))

    def test_verify_detects_event_in_two_target_partitions(self) -> None:
        """支付事件同时落入两个目标分区时核对失败。"""
        plan = self._completed_plan("LAKE-I003")
        # 人为制造冲突：把批次1的一个旧小时分区塞进批次2
        first, second = plan["batches"][0], plan["batches"][1]
        second["source_partitions"].append(first["source_partitions"][0])
        result, message = self.iceberg_svc.verify_target_metadata(3)
        self.assertIsNone(result)
        self.assertIn("两个目标分区", message)


class TestMigrationChain(unittest.TestCase):
    """跨模块链路：以分区迁移计划为交接对象连接湖上分区与Iceberg管理"""

    def setUp(self) -> None:
        self.lake_svc = LakePartitionService()
        self.iceberg_svc = IcebergTableService()

    def test_chain_ready_after_verify(self) -> None:
        entry = _create_hourly_payment_partition(self.lake_svc, "LAKE-C001")
        plan, _ = self.lake_svc.generate_migration_plan(entry["id"], "switch_metadata_first")
        migration_service.execute_plan(plan["plan_id"], batch_limit=7)
        self.iceberg_svc.verify_target_metadata(3)

        status = pipeline_engine.migration_chain_status("payments")
        self.assertTrue(status["lake_partition"]["exists"])
        self.assertTrue(status["migration_plan"]["exists"])
        self.assertEqual(status["migration_plan"]["status"], "已完成")
        self.assertEqual(status["migration_plan"]["progress"], "7/7")
        self.assertTrue(status["iceberg_table"]["verified"])
        self.assertTrue(status["chain_ready"])

    def test_apply_partition_uses_completed_plan(self) -> None:
        """分区应用以迁移计划为交接对象写入目标规格。"""
        entry = _create_hourly_payment_partition(self.lake_svc, "LAKE-C002")
        plan, _ = self.lake_svc.generate_migration_plan(entry["id"], "archive_boundary_first")
        migration_service.execute_plan(plan["plan_id"], batch_limit=7)

        result = pipeline_engine.apply_partition_to_iceberg("payments")
        self.assertTrue(result["ok"])
        spec = result["spec"]
        self.assertEqual(spec["granularity"], "day")
        self.assertEqual(spec["plan_id"], plan["plan_id"])
        self.assertEqual(spec["target_spec"], "date_trunc('day', pay_time)")
        self.assertEqual(spec["source"], "partition_migration_plan")


if __name__ == "__main__":
    unittest.main()
