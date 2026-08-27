from __future__ import annotations

import os
import hmac
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from .database import build_target_table_name_map, default_schema, list_objects, list_owners, list_tables, make_engine, resolve_table_name_policy, table_name_case_capabilities, test_engine
from .models import ConnectionConfig, CreateJobRequest, DeepAssessmentRequest, JobEditTemplate, JobLogResponse, JobSummary, MigrationLinkCreate, MigrationLinkSummary, MigrationLinkUpdate, PaginatedJobs, TableInfo, TableListRequest, TestConnectionRequest
from .assessment import assess_payload
from .assessment_deep import deep_assess_payload, export_deep_report
from .cdc import OracleLogMiner, make_logminer_engine
from .store import build_store, utc_now
from .worker import MigrationRunner
from .validation import validate_payload

store = build_store()
runner = MigrationRunner(store, workers=int(os.environ.get("FLOWDB_WORKERS", "2")))


def resolved_migration_payload(request: CreateJobRequest) -> dict:
    payload = request.model_dump()
    if request.link_id:
        link = store.get_link_payload(request.link_id)
        payload["source"] = link["source"]
        payload["target"] = link["target"]
        payload["link_name"] = link["name"]
    if not payload.get("source") or not payload.get("target"):
        raise HTTPException(status_code=400, detail="迁移链路缺少源端或目标端连接")
    payload["source_type"] = (payload.get("source") or {}).get("type", "")
    return payload


def apply_identifier_case_configuration(payload: dict) -> dict:
    """Detect immutable target behavior, resolve the requested policy and preflight collisions."""
    target = ConnectionConfig.model_validate(payload["target"])
    engine = make_engine(target)
    try:
        capabilities = table_name_case_capabilities(engine)
    finally:
        engine.dispose()
    requested = payload.get("identifier_case_policy", "auto")
    lower_case_table_names = capabilities["lower_case_table_names"]
    resolved = resolve_table_name_policy(requested, lower_case_table_names)
    payload["identifier_case_resolved"] = resolved
    payload["target_lower_case_table_names"] = lower_case_table_names
    payload["target_object_names"] = build_target_table_name_map(
        payload.get("tables", []), resolved, lower_case_table_names
    )
    return payload


def connection_error_detail(exc: Exception) -> dict[str, str]:
    cause = str(exc).replace("\n", " ")[:500]
    code_match = re.search(r"(?:ORA|DPY)-\d+", cause)
    code = code_match.group(0) if code_match else "CONNECTION_ERROR"
    rules = (
        (("ORA-01017", "DPY-4001"), "用户名或密码错误", "确认用户名、密码和大小写后重试。"),
        (("ORA-12514", "ORA-12505"), "Oracle 服务名不存在或未向监听器注册", "确认填写的是 Service Name（本环境为 pdb01），不是 SID。"),
        (("ORA-12541",), "Oracle 监听器未启动或端口不可达", "检查主机、1521 端口、防火墙和 listener 状态。"),
        (("ORA-12170", "DPY-6005", "timed out", "timeout"), "数据库网络连接超时", "确认迁移节点与数据库处于可达网络，且防火墙允许访问。"),
        (("ORA-28000",), "Oracle 用户已锁定", "请解锁用户后重试。"),
        (("ORA-28001",), "Oracle 用户密码已过期", "请修改密码后重试。"),
    )
    lowered = cause.lower()
    for needles, message, hint in rules:
        if any(needle.lower() in lowered for needle in needles):
            return {"code": code, "message": message, "cause": cause, "hint": hint}
    return {"code": code, "message": "数据库连接失败", "cause": cause, "hint": "请检查迁移节点日志、连接参数和数据库网络。"}


@asynccontextmanager
async def lifespan(_: FastAPI):
    for job in store.list(100):
        if job["status"] in {"catching_up", "syncing"} and job.get("sync_mode") != "full_only":
            runner.submit(job["id"])
        elif job["status"] in {"queued", "running"}:
            store.update(job["id"], status="failed", error="服务重启导致任务中断，请重新创建任务", finished_at=None)
    yield


app = FastAPI(title="FlowDB Migration API", version="1.0.0", lifespan=lifespan)
origins = [item.strip() for item in os.environ.get("FLOWDB_CORS_ORIGINS", "http://localhost:8080,http://localhost:3000").split(",") if item.strip()]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["GET", "POST", "PUT", "DELETE"], allow_headers=["*"])


@app.middleware("http")
async def require_api_token(request, call_next):
    expected = os.environ.get("FLOWDB_API_TOKEN", "")
    if request.url.path != "/health" and expected:
        supplied = request.headers.get("x-flowdb-token", "")
        if not hmac.compare_digest(supplied, expected):
            return JSONResponse(status_code=401, content={"detail": "迁移节点令牌无效"})
    return await call_next(request)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/connections/test")
def test_connection(request: TestConnectionRequest):
    engine = make_engine(request.connection)
    started = time.perf_counter()
    try:
        dialect, version = test_engine(engine)
        return {"ok": True, "dialect": dialect, "version": version, "latency_ms": round((time.perf_counter() - started) * 1000), **table_name_case_capabilities(engine)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=connection_error_detail(exc)) from exc
    finally:
        engine.dispose()


@app.post("/api/connections/tables", response_model=list[TableInfo])
def tables(request: TableListRequest):
    engine = make_engine(request.connection)
    try:
        return list_tables(engine, default_schema(request.connection), owners=request.owners)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"读取表失败：{str(exc)[:500]}") from exc
    finally:
        engine.dispose()


@app.post("/api/connections/objects", response_model=list[TableInfo])
def objects(request: TableListRequest):
    engine = make_engine(request.connection)
    try:
        return list_objects(engine, default_schema(request.connection), owners=request.owners)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"读取表和视图失败：{str(exc)[:500]}") from exc
    finally:
        engine.dispose()


@app.post("/api/connections/owners", response_model=list[str])
def connection_owners(request: TableListRequest):
    engine = make_engine(request.connection)
    try:
        return list_owners(engine, request.connection)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"读取 owner 列表失败：{str(exc)[:500]}") from exc
    finally:
        engine.dispose()


@app.get("/api/links", response_model=list[MigrationLinkSummary])
def links():
    return store.list_links()


@app.post("/api/links", response_model=MigrationLinkSummary, status_code=201)
def create_link(request: MigrationLinkCreate):
    try:
        return store.create_link(uuid.uuid4().hex, request.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.put("/api/links/{link_id}", response_model=MigrationLinkSummary)
def update_link(link_id: str, request: MigrationLinkUpdate):
    try:
        previous = store.get_link_payload(link_id)
        payload = request.model_dump()
        for side in ("source", "target"):
            if not payload[side].get("password"):
                payload[side]["password"] = previous[side]["password"]
        return store.update_link(link_id, payload)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="链路不存在") from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.delete("/api/links/{link_id}", status_code=204)
def delete_link(link_id: str):
    try:
        store.delete_link(link_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="链路不存在") from exc


@app.post("/api/links/{link_id}/test")
def test_link(link_id: str):
    try:
        payload = store.get_link_payload(link_id)
        result = {}
        for side in ("source", "target"):
            config = ConnectionConfig.model_validate(payload[side])
            engine = make_engine(config)
            try:
                dialect, version = test_engine(engine)
                result[side] = {"ok": True, "dialect": dialect, "version": version, **table_name_case_capabilities(engine)}
            finally:
                engine.dispose()
        return result
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="链路不存在") from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=connection_error_detail(exc)) from exc


@app.get("/api/links/{link_id}/objects", response_model=list[TableInfo])
def link_objects(link_id: str, owners: list[str] | None = Query(default=None, max_length=50)):
    try:
        payload = store.get_link_payload(link_id)
        config = ConnectionConfig.model_validate(payload["source"])
        engine = make_engine(config)
        try:
            return list_objects(engine, default_schema(config), owners=owners)
        finally:
            engine.dispose()
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="链路不存在") from exc


@app.get("/api/links/{link_id}/owners", response_model=list[str])
def link_owners(link_id: str):
    try:
        payload = store.get_link_payload(link_id)
        config = ConnectionConfig.model_validate(payload["source"])
        engine = make_engine(config)
        try:
            return list_owners(engine, config)
        finally:
            engine.dispose()
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="链路不存在") from exc


@app.get("/api/links/{link_id}/cdc-capabilities")
def link_cdc_capabilities(link_id: str):
    """Read-only Oracle LogMiner readiness check used before creating a sync task."""
    try:
        payload = store.get_link_payload(link_id)
        source = ConnectionConfig.model_validate(payload["source"])
        target = ConnectionConfig.model_validate(payload["target"])
        if source.type != "oracle" or target.type not in {"mysql", "tdsql"}:
            raise HTTPException(status_code=400, detail="增量同步当前仅支持 Oracle → MySQL/TDSQL")
        engine = make_logminer_engine(source)
        try:
            miner = OracleLogMiner(engine)
            return {**miner.capabilities(), "current_scn": miner.current_scn()}
        finally:
            engine.dispose()
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="链路不存在") from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"LogMiner 能力检查失败：{str(exc)[:800]}") from exc


@app.post("/api/jobs", response_model=JobSummary, status_code=202)
def create_job(request: CreateJobRequest):
    try:
        payload = apply_identifier_case_configuration(resolved_migration_payload(request))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="保存的迁移链路不存在") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    source = payload["source"]
    target = payload["target"]
    if source["type"] == target["type"] and source["host"] == target["host"] and source["database"] == target["database"] and source.get("schema_name") == target.get("schema_name"):
        raise HTTPException(status_code=400, detail="源端与目标端不能是同一数据库和 Schema")
    job_id = uuid.uuid4().hex
    job = store.create(job_id, payload)
    runner.submit(job_id)
    return job


@app.get("/api/jobs", response_model=list[JobSummary])
def jobs(limit: int = Query(default=30, ge=1, le=100)):
    return store.list(limit)


@app.get("/api/jobs/page", response_model=PaginatedJobs)
def paginated_jobs(page: int = Query(default=1, ge=1), page_size: int = Query(default=10, ge=5, le=50)):
    total = store.count()
    pages = max(1, (total + page_size - 1) // page_size)
    safe_page = min(page, pages)
    return {"items": store.list(page_size, (safe_page - 1) * page_size), "total": total, "page": safe_page, "page_size": page_size, "pages": pages}


@app.post("/api/assessments")
def assess_migration(request: CreateJobRequest):
    try:
        return assess_payload(apply_identifier_case_configuration(resolved_migration_payload(request)))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"迁移评估失败：{str(exc)[:800]}") from exc


@app.post("/api/assessments/deep")
def deep_assessment(request: DeepAssessmentRequest):
    try:
        source = request.source.model_dump() if request.source else None
        target = request.target.model_dump() if request.target else None
        if request.link_id:
            link = store.get_link_payload(request.link_id)
            source = link["source"]
            target = link["target"]
        if not source or not target:
            raise HTTPException(status_code=400, detail="深度评估缺少源端或目标端连接")
        return deep_assess_payload(
            source, target,
            owners=request.owners,
            bandwidth_mbps=request.bandwidth_mbps,
            batch_size=request.batch_size,
            table_concurrency=request.table_concurrency,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"深度评估失败：{str(exc)[:800]}") from exc


@app.get("/api/jobs/{job_id}", response_model=JobSummary)
def job(job_id: str):
    try:
        return store.get(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc


@app.post("/api/jobs/{job_id}/cancel", response_model=JobSummary)
def cancel_job(job_id: str):
    try:
        current = store.get(job_id)
        if current["status"] == "cancelled":
            return current
        if current["status"] not in {"queued", "running", "catching_up", "syncing"}:
            raise HTTPException(status_code=409, detail="当前任务不能取消")
        store.update(
            job_id,
            cancel_requested=1,
            status="cancelled",
            current_table=None,
            finished_at=utc_now(),
        )
        runner.request_cancel(job_id)
        store.append_log(job_id, "WARN", "用户确认取消，任务已停止")
        return store.get(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc


@app.post("/api/jobs/{job_id}/finish-sync", response_model=JobSummary)
def finish_sync_job(job_id: str):
    """Normally finish a full-and-incremental job after it reaches CDC."""
    try:
        current = store.get(job_id)
        if current["status"] == "completed":
            return current
        if current.get("sync_mode") not in {"full_and_incremental", "incremental_only"} or current["status"] not in {
            "catching_up",
            "syncing",
        }:
            raise HTTPException(status_code=409, detail="只有进入增量阶段的任务才能结束同步")
        store.update(
            job_id,
            status="completed",
            sync_phase="stopped",
            progress=100,
            current_table=None,
            finished_at=utc_now(),
        )
        runner.request_cancel(job_id)
        store.append_log(
            job_id,
            "INFO",
            "用户正常结束实时同步，任务已完成："
            f"最终检查点 SCN={current.get('checkpoint_scn') or '-'}，"
            f"累计事务={current.get('cdc_transactions') or 0}，"
            f"累计 DML={current.get('cdc_events') or 0}（"
            f"新增={current.get('cdc_inserts') or 0}，"
            f"更新={current.get('cdc_updates') or 0}，"
            f"删除={current.get('cdc_deletes') or 0}）",
        )
        return store.get(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc


@app.post("/api/jobs/{job_id}/start-incremental", response_model=JobSummary, status_code=202)
def start_incremental_job(job_id: str):
    """Create a separate incremental task from a successful retained full baseline."""
    try:
        current = store.get(job_id)
        if (
            current.get("sync_mode") != "full_then_incremental"
            or current.get("status") != "completed"
            or current.get("sync_phase") != "ready_for_incremental"
            or not current.get("start_scn")
        ):
            raise HTTPException(
                status_code=409,
                detail="只有全量成功且已保留 SCN 的任务才能启动增量同步",
            )
        payload = store.get_payload(job_id)
        incremental_payload = {
            **payload,
            "name": f"{payload.get('name') or current['name']}（增量同步）",
            "sync_mode": "incremental_only",
            "start_scn": int(current["start_scn"]),
            "migration_content": "data_only",
            "existing_table": "append",
            "create_tables": False,
            "sequences": [],
            "migrate_sequences": False,
        }
        incremental_id = uuid.uuid4().hex
        created = store.create(incremental_id, incremental_payload)
        store.update(job_id, sync_phase="incremental_started")
        store.append_log(
            job_id,
            "INFO",
            f"已基于保留位点 SCN={current['start_scn']} 创建独立增量任务 {incremental_id}",
        )
        runner.submit(incremental_id)
        return created
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc


@app.post("/api/jobs/{job_id}/resume-incremental", response_model=JobSummary, status_code=202)
def resume_incremental_job(job_id: str):
    """Continue a stopped manual CDC task from its durable checkpoint SCN."""
    try:
        current = store.get(job_id)
        if (
            current.get("sync_mode") != "incremental_only"
            or current.get("status") not in {"completed", "cancelled"}
            or current.get("sync_phase") != "stopped"
            or not current.get("checkpoint_scn")
        ):
            raise HTTPException(
                status_code=409,
                detail="只有已结束且保存了检查点 SCN 的手动增量任务才能继续同步",
            )
        checkpoint_scn = int(current["checkpoint_scn"])
        store.update(
            job_id,
            status="queued",
            sync_phase="incremental",
            cancel_requested=0,
            current_table="等待从检查点继续增量同步",
            finished_at=None,
            error=None,
            progress=100,
            tables_completed=current.get("tables_total", 0),
        )
        store.append_log(job_id, "INFO", f"用户继续增量同步，将从检查点 SCN={checkpoint_scn} 恢复")
        runner.submit_when_idle(job_id)
        return store.get(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc


@app.get("/api/jobs/{job_id}/logs", response_model=JobLogResponse)
def job_logs(job_id: str, after_seq: int = Query(default=0, ge=0)):
    try:
        store.get(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc
    logs = store.get_logs(job_id, after_seq=after_seq)
    latest = logs[-1]["seq"] if logs else after_seq
    return {"job_id": job_id, "after_seq": latest, "logs": logs}


@app.get("/api/jobs/{job_id}/edit", response_model=JobEditTemplate)
def job_edit_template(job_id: str):
    """返回任务创建参数模板用于『编辑并重试』，密码字段不返回，沿用链路凭据。"""
    try:
        payload = store.get_payload(job_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc
    return {
        "id": job_id,
        "name": payload.get("name", ""),
        "link_id": payload.get("link_id"),
        "link_name": payload.get("link_name"),
        "tables": payload.get("tables", []),
        "object_types": payload.get("object_types", {}),
        "sequences": payload.get("sequences", []),
        "batch_size": payload.get("batch_size", 2000),
        "table_concurrency": payload.get("table_concurrency", 1),
        "existing_table": payload.get("existing_table", "fail"),
        "create_tables": payload.get("create_tables", True),
        "migration_content": payload.get("migration_content", "structure_and_data"),
        "fail_policy": payload.get("fail_policy", "stop_on_error"),
        "migrate_sequences": payload.get("migrate_sequences", True),
        "user_mappings": payload.get("user_mappings", []),
        "identifier_case_policy": payload.get("identifier_case_policy", "auto"),
        "sync_mode": payload.get("sync_mode", "full_only"),
        "start_scn": payload.get("start_scn"),
        "cdc_poll_seconds": payload.get("cdc_poll_seconds", 3.0),
        "cdc_key_overrides": payload.get("cdc_key_overrides", {}),
        "cdc_no_key_policy": payload.get("cdc_no_key_policy", "reject"),
        "cdc_allow_source_ddl": payload.get("cdc_allow_source_ddl", False),
        "source_type": payload.get("source_type", ""),
    }


@app.post("/api/jobs/{job_id}/retry", response_model=JobSummary, status_code=202)
def retry_job(job_id: str):
    try:
        current = store.get(job_id)
        if current["status"] not in {"failed", "cancelled"}:
            raise HTTPException(status_code=409, detail="只有失败或已取消的任务可以重试")
        payload = store.get_payload(job_id)
        payload["name"] = f"{payload['name']}（重试）"[:120]
        payload.setdefault("migration_content", "structure_and_data")
        apply_identifier_case_configuration(payload)
        new_id = uuid.uuid4().hex
        job = store.create(new_id, payload)
        runner.submit(new_id)
        return job
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc


@app.post("/api/jobs/{job_id}/validate")
def validate_job(job_id: str):
    try:
        current = store.get(job_id)
        if current["status"] != "completed":
            raise HTTPException(status_code=409, detail="只有已完成任务可以执行校验")
        result = validate_payload(store.get_payload(job_id))
        return store.create_validation(
            uuid.uuid4().hex,
            job_id,
            current["name"],
            result,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="任务不存在") from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"校验失败：{str(exc)[:500]}") from exc


@app.get("/api/validations/page")
def validation_history_page(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=5, le=50),
    status: str = Query(default="all", pattern="^(all|passed|failed)$"),
):
    total = store.count_validations(status)
    pages = max(1, (total + page_size - 1) // page_size)
    current_page = min(page, pages)
    return {
        "items": store.list_validations(
            status, page_size, (current_page - 1) * page_size
        ),
        "total": total,
        "page": current_page,
        "page_size": page_size,
        "pages": pages,
    }


@app.get("/api/validations/{validation_id}")
def validation_history_detail(validation_id: str):
    try:
        return store.get_validation(validation_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="校验记录不存在") from exc


@app.post("/api/assessments/deep/export")
def deep_assessment_export(request: DeepAssessmentRequest):
    """生成完整深度评估报告的 Markdown 文件并落盘到 outputs 目录。"""
    try:
        source = request.source.model_dump() if request.source else None
        target = request.target.model_dump() if request.target else None
        if request.link_id:
            link = store.get_link_payload(request.link_id)
            source = link["source"]
            target = link["target"]
        if not source or not target:
            raise HTTPException(status_code=400, detail="导出缺少源端或目标端连接")
        payload = deep_assess_payload(source, target, bandwidth_mbps=request.bandwidth_mbps, owners=request.owners)
        root = Path(__file__).resolve().parent.parent.parent
        report_dir = root / "outputs"
        report = export_deep_report(payload, report_dir)
        return report
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"报告导出失败：{str(exc)[:800]}") from exc


@app.get("/api/reports/{file_name}")
def download_report(file_name: str):
    """下载已导出的深度评估报告（Markdown / HTML）。"""
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.(md|html)", file_name):
        raise HTTPException(status_code=400, detail="非法的报告文件名")
    root = Path(__file__).resolve().parent.parent.parent
    report_dir = root / "outputs"
    file_path = report_dir / file_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="报告文件不存在")
    media_type = "text/html; charset=utf-8" if file_name.endswith(".html") else "text/markdown; charset=utf-8"
    return FileResponse(str(file_path), media_type=media_type, filename=file_name)
