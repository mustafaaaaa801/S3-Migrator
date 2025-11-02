# backend/app/main.py
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
import sqlite3, os, threading, subprocess, json
from migrator import Migrator

app = FastAPI()
DB = '/data/migrate.db'

class ConfigIn(BaseModel):
    aws_key: str
    aws_secret: str
    aws_region: str = 'us-east-1'
    src_bucket: str
    target_endpoint: str
    target_key: str
    target_secret: str
    target_bucket: str = None
    max_workers: int = 8
    dry_run: bool = False

# save config into file (restrict perms!)
CONFIG_PATH = '/data/config.json'

@app.post('/api/config')
async def save_config(cfg: ConfigIn):
    data = cfg.dict()
    if not data.get('target_bucket'):
        data['target_bucket'] = data['src_bucket']
    # persist
    with open(CONFIG_PATH, 'w') as f:
        json.dump(data, f)
    os.chmod(CONFIG_PATH, 0o600)
    return {'ok': True}

# start migration
@app.post('/api/start')
async def start_migration(background_tasks: BackgroundTasks):
    if not os.path.exists(CONFIG_PATH):
        raise HTTPException(status_code=400, detail='config missing')
    # run in background
    def runner():
        m = Migrator(CONFIG_PATH, db_path=DB)
        m.run_sync()
    t = threading.Thread(target=runner, daemon=True)
    t.start()
    return {'started': True}

@app.get('/api/status')
def status():
    # return simple status from DB
    if not os.path.exists(DB):
        return {'ready': False}
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT status, COUNT(*) FROM objects GROUP BY status")
    rows = cur.fetchall()
    conn.close()
    return {'summary': rows}

@app.get('/api/logs')
def logs():
    logp = '/data/s3_migrator.log'
    if not os.path.exists(logp):
        return {'logs': ''}
    with open(logp,'r') as f:
        return {'logs': f.read()[-10000:]}