#!/bin/bash
# Run inside Docker container to generate dbt docs

import_creds() {
    python3 -c "
import tomllib
from urllib.parse import urlparse
with open('/opt/airflow/.dlt/secrets.toml', 'rb') as f:
    raw = tomllib.load(f)
p = urlparse(raw['destinations']['warehouse']['credentials'])
print(f'export WAREHOUSE_HOST={p.hostname}')
print(f'export WAREHOUSE_PORT={p.port or 5432}')
print(f'export WAREHOUSE_USER={p.username}')
print(f'export WAREHOUSE_PASSWORD={p.password}')
print(f'export WAREHOUSE_DB={(p.path or \"\").lstrip(\"/\")}')
"
}

eval "$(import_creds)"

cd /opt/airflow/dbt
dbt docs generate --profiles-dir .
