import os
from pydantic import BaseSettings

class Settings(BaseSettings):
    # DB
    DATABASE_URL: str = os.getenv('DATABASE_URL', 'sqlite:///./migrate.db')
    # Celery
    CELERY_BROKER_URL: str = os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0')
    # S3 placeholders (do NOT set secrets here in repo)
    AWS_REGION: str = os.getenv('AWS_REGION', 'us-east-1')
    # paths
    TEMP_DIR: str = os.getenv('TEMP_DIR', '/tmp/s3_migrator')
    LOG_FILE: str = os.getenv('LOG_FILE', '/data/s3_migrator.log')

settings = Settings()