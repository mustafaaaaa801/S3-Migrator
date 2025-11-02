from celery import Celery
from .migrator import Migrator
import json, os

celery = Celery('worker', broker=os.getenv('CELERY_BROKER_URL'), backend=os.getenv('CELERY_RESULT_BACKEND','rpc://'))

@celery.task(bind=True)
def start_migration_task(self, cfg_path):
    # load config, resolve secrets (here we assume secrets are in env for demo)
    with open(cfg_path) as f:
        cfg = json.load(f)
    # resolve secrets: in production use Vault or similar
    aws_key = os.getenv('AWS_KEY')
    aws_secret = os.getenv('AWS_SECRET')
    target_key = os.getenv('TARGET_KEY')
    target_secret = os.getenv('TARGET_SECRET')
    mig = Migrator(cfg, aws_key, aws_secret, target_key, target_secret, region=os.getenv('AWS_REGION','us-east-1'))
    count = 0
    for o in mig.scan_source():
        key = o['Key']
        try:
            ok, sha = mig.transfer_object(key)
            count += 1
            self.update_state(state='PROGRESS', meta={'processed': count, 'last_key': key})
        except Exception as e:
            # log and continue
            continue
    return {'processed': count}