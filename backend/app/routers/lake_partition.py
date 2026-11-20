"""湖上分区接口：维护分区定义，覆盖提交校验、标记生效、停用分区等动作。

增强：新增分区迁移计划接口——生成、执行、中断、恢复，计划是交给Iceberg管理的交接对象。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.lake_partition import LakePartitionService
from app.services.partition_migration import migration_service

router = APIRouter(prefix="/api/lake_partition", tags=["湖上分区"])

service = LakePartitionService()

LIST_FIELDS = ["分区编号", "目标表名", "分区列名", "分区粒度", "分区转换", "存储位置", "文件格式", "生命周期"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按分区编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已生效、已停用"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、分区编号关键字与状态过滤湖上分区列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="lake_partition.csv"'})


# ───────────────────────────────
# 分区迁移计划：必须声明在 /{entry_id} 之前，避免被整数路由吃掉
# ───────────────────────────────

@router.get("/migration-plans", response_model=PageResult[dict])
def list_migration_plans(
    table_name: str | None = Query(default=None, description="按目标表名过滤"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=200),
) -> PageResult[dict]:
    """分区迁移计划列表；空列表时前端展示暂无记录。"""
    plans = migration_service.list_plans(table_name)
    total = len(plans)
    start = (page - 1) * size
    return PageResult(items=plans[start:start + size], total=total, page=page, size=size)


@router.post("/migration-plans", response_model=ActionResult)
def generate_migration_plan(payload: dict[str, Any]) -> ActionResult:
    """校验支付表分区定义并生成迁移计划：保留旧分区边界与目标分区规格，旧小时分区与目标日分区一一对应。"""
    entry_id = payload.get("entry_id")
    if not isinstance(entry_id, int):
        return ActionResult(ok=False, message="缺少 entry_id，无法定位分区定义")
    strategy = str(payload.get("strategy") or "archive_boundary_first")
    plan, message = service.generate_migration_plan(entry_id, strategy)
    if plan is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=plan)


@router.get("/migration-plans/{plan_id}", response_model=dict)
def get_migration_plan(plan_id: str) -> dict:
    """读取单条迁移计划明细（含批次与一一对应映射）；不存在时给出可读的错误说明。"""
    plan = migration_service.get_plan(plan_id)
    if plan is None:
        raise HTTPException(status_code=404, detail=f"分区迁移计划 {plan_id} 不存在")
    return plan


@router.post("/migration-plans/{plan_id}/execute", response_model=ActionResult)
def execute_migration_plan(plan_id: str, payload: dict[str, Any] | None = None) -> ActionResult:
    """按批次执行迁移；batch_limit 缺省为 1，即每次执行一个未完成批次。"""
    body = payload or {}
    raw_limit = body.get("batch_limit")
    batch_limit = raw_limit if isinstance(raw_limit, int) and raw_limit > 0 else 1
    plan, message = migration_service.execute_plan(plan_id, batch_limit=batch_limit)
    if plan is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=plan)


@router.post("/migration-plans/{plan_id}/interrupt", response_model=ActionResult)
def interrupt_migration_plan(plan_id: str, payload: dict[str, Any] | None = None) -> ActionResult:
    """标记中断并说明原因；已迁移范围保留，恢复后只续做未完成批次。"""
    reason = (payload or {}).get("reason")
    plan, message = migration_service.interrupt_plan(plan_id, reason)
    if plan is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=plan)


@router.post("/migration-plans/{plan_id}/resume", response_model=ActionResult)
def resume_migration_plan(plan_id: str, payload: dict[str, Any] | None = None) -> ActionResult:
    """中断或失败后重试：只续做未完成批次，返回消息说明恢复原因。"""
    reason = (payload or {}).get("reason")
    plan, message = migration_service.resume_plan(plan_id, reason)
    if plan is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=plan)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条分区定义明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"分区定义 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条分区定义，缺字段或校验未通过时说明原因而不是静默丢弃。"""
    entry, errors = service.create_entry(payload)
    if errors:
        return ActionResult(ok=False, message="；".join(errors))
    return ActionResult(ok=True, message="分区定义已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条分区定义执行提交校验、标记生效、停用分区；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
