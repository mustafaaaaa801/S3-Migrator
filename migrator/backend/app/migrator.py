import os, hashlib, tempfile, logging
import boto3
from botocore.client import Config
from boto3.s3.transfer import TransferConfig

logging.basicConfig(level=logging.INFO)

class Migrator:
    def __init__(self, cfg, aws_access_key, aws_secret_key, target_key, target_secret, region='us-east-1'):
        self.cfg = cfg
        self.region = region
        # source client using AWS creds
        self.src = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region, config=Config(signature_version='s3v4'))
        # target client (S3 compatible)
        self.tgt = boto3.client('s3', endpoint_url=cfg['target_endpoint'], aws_access_key_id=target_key, aws_secret_access_key=target_secret, config=Config(signature_version='s3v4'))
        self.temp_dir = os.getenv('TEMP_DIR', '/tmp/s3_migrator')
        os.makedirs(self.temp_dir, exist_ok=True)

    def scan_source(self):
        paginator = self.src.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.cfg['src_bucket']):
            for o in page.get('Contents', []):
                yield o

    def compute_sha256(self, path):
        h = hashlib.sha256()
        with open(path,'rb') as f:
            for chunk in iter(lambda: f.read(8*1024*1024), b''):
                h.update(chunk)
        return h.hexdigest()

    def transfer_object(self, key):
        # download to temp file
        tmp = tempfile.NamedTemporaryFile(dir=self.temp_dir, delete=False)
        try:
            resp = self.src.get_object(Bucket=self.cfg['src_bucket'], Key=key)
            stream = resp['Body']
            for chunk in iter(lambda: stream.read(8*1024*1024), b''):
                tmp.write(chunk)
            tmp.flush(); tmp.close()
            sha = self.compute_sha256(tmp.name)
            # upload with transfer config
            mp_threshold = self.cfg.get('multipart_threshold', 50*1024*1024)
            transfer_config = TransferConfig(multipart_threshold=mp_threshold, multipart_chunksize=8*1024*1024)
            self.tgt.upload_file(tmp.name, self.cfg.get('target_bucket', self.cfg['src_bucket']), key, ExtraArgs={'Metadata': {'sha256': sha}}, Config=transfer_config)
            return True, sha
        finally:
            try: os.remove(tmp.name)
            except: pass