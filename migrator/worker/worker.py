from celery import Celery
import os


celery = Celery('worker', broker=os.getenv('CELERY_BROKER_URL'))
celery.conf.update(result_backend=os.getenv('CELERY_RESULT_BACKEND','rpc://'))


if __name__ == '__main__':
    celery.worker_main(['worker', '--loglevel=info'])