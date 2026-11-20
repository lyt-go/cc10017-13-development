"""Iceberg管理接口：维护表元数据，覆盖提交校验、确认就绪、标记下线等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.iceberg_table import IcebergTableService

router = APIRouter(prefix="/api/iceberg_table", tags=["Iceberg管理"])

service = IcebergTableService()

LIST_FIELDS = ["表编号", "表名称", "存储格式", "分区策略", "排序字段", "快照过期", "孤立文件清理", "元数据版本"]
STATUSES = ["草稿", "待校验", "已就绪", "已下线"]


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
    keyword: str | None = Query(default=None, description="按表编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已就绪、已下线"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、表编号关键字与状态过滤Iceberg管理列表。"""
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
        headers={"Content-Disposition": 'attachment; filename="iceberg_table.csv"'})


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条表元数据明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"表元数据 {entry_id} 不存在或已归档")
    return entry


@router.get("/{entry_id}/migration-plans", response_model=PageResult[dict])
def list_related_migration_plans(entry_id: int) -> PageResult[dict]:
    """表关联的分区迁移计划：与湖上分区读取同一份交接对象，保证两边展示一致。"""
    plans = service.list_migration_plans(entry_id)
    if plans is None:
        raise HTTPException(status_code=404, detail=f"表元数据 {entry_id} 不存在或已归档")
    return PageResult(items=plans, total=len(plans), page=1, size=len(plans) or 1)


@router.post("/{entry_id}/verify-migration", response_model=ActionResult)
def verify_migration(entry_id: int) -> ActionResult:
    """核对最新分区迁移计划并写入支付表目标分区元数据；计划未完成或落区冲突时说明原因。"""
    entry, message = service.verify_target_metadata(entry_id)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条表元数据，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="表元数据已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条表元数据执行提交校验、确认就绪、标记下线；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
