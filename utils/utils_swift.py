import gzip
import os
import pandas as pd
import swiftclient

from io import BytesIO, TextIOWrapper
from retry import retry

from application.server.main.logger import get_logger

logger = get_logger(__name__)
SWIFT_SIZE = 10000
key = os.getenv('OS_PASSWORD')
project_name = os.getenv('OS_PROJECT_NAME')
project_id = os.getenv('OS_TENANT_ID')
tenant_name = os.getenv('OS_TENANT_NAME')
username = os.getenv('OS_USERNAME')
user = f'{tenant_name}:{username}'
init_cmd = f"swift --os-auth-url https://auth.cloud.ovh.net/v3 --auth-version 3 \
      --key {key}\
      --user {user} \
      --os-user-domain-name Default \
      --os-project-domain-name Default \
      --os-project-id {project_id} \
      --os-project-name {project_name} \
      --os-region-name GRA"
conn = None

MOUNTED_VOLUME = '/data'

@retry(delay=2, tries=50)
def upload_object(container: str, filename: str) -> str:
    object_name = filename.split('/')[-1]
    logger.debug(f'Uploading {filename} in {container} as {object_name}')
    cmd = init_cmd + f' upload {container} {filename} --object-name {object_name}' \
                     f' --segment-size 1048576000 --segment-threads 100'
    os.system(cmd)
    return f'https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/{container}/{object_name}'


@retry(delay=2, tries=50)
def download_object(container: str, filename: str, out: str) -> None:
    logger.debug(f'Downloading {filename} from {container} to {out}')
    cmd = init_cmd + f' download {container} {filename} -o {out}'
    os.system(cmd)

@retry(delay=2, tries=50)
def download_container(container, download_prefix):
    cmd =  init_cmd + f' download {container} -D {MOUNTED_VOLUME}/{container} --skip-identical'
    if download_prefix:
        cmd += f" --prefix {download_prefix}"
    os.system(cmd)
    if download_prefix:
        return f'{MOUNTED_VOLUME}/{container}/{download_prefix}'
    return f'{MOUNTED_VOLUME}/{container}'
