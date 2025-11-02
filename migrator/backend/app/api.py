from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from .tasks import start_migration_task
from .db import get_session
from sqlmodel import select

router = APIRouter()

class ConfigIn(BaseModel):
    name: str
    src_bucket: str
    # secrets referenced by name in secret manager
    aws_secret_ref: str
    target_endpoint: str
    target_bucket: str | None = None
    target_secret_ref: str
    max_workers: int = 8
    dry_run: bool = True

@router.post('/config')
def save_config(cfg: ConfigIn):
    # here you should save config to DB (omitted for brevity) or secret reference
    # For demo: simply write to file under /data/configs/{name}.json
    import json, os
    os.makedirs('/data/configs', exist_ok=True)
    path = f'/data/configs/{cfg.name}.json'
    with open(path, 'w') as f:
        json.dump(cfg.dict(), f)
    return {'ok': True, 'path': path}

@router.post('/start/{name}')
def start(name: str):
    cfg_path = f'/data/configs/{name}.json'
    import os
    if not os.path.exists(cfg_path):
        raise HTTPException(status_code=404, detail='config not found')
    # enqueue celery task
    task = start_migration_task.delay(cfg_path)
    return {'task_id': task.id}

@router.get('/status/{task_id}')
def status(task_id: str):
    from celery.result import AsyncResult
    res = AsyncResult(task_id)
    return {'state': res.state, 'info': str(res.info)}