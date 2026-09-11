"""Audit this small test's physical Parquet against the Spark file sink manifest."""
import json
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow.parquet as pq


name = sys.argv[1]
root = Path('/opt/airflow/data/streaming') / name
manifest = root / '_spark_metadata' / '0'
entries = [json.loads(line) for line in manifest.read_text().splitlines()[1:]]
committed = {Path(unquote(urlparse(e['path']).path)).relative_to('/opt/spark/data/streaming/' + name)
             for e in entries if e['action'] == 'add'}
physical = {p.relative_to(root) for p in root.rglob('*.parquet')}
orphans = physical - committed
missing = committed - physical
assert not missing, missing
rows = []
unreadable = []
for p in sorted(physical):
    try:
        table = pq.ParquetFile(root / p).read()
        if p in committed:
            rows.extend(table.to_pylist())
    except Exception as exc:
        unreadable.append({'file': str(p), 'error': str(exc)})
coords = {(r['topic'], r['partition'], r['offset']) for r in rows}
assert len(rows) == len(coords) == 16
assert sum(not r['validation_errors'] for r in rows) == 10
ids = {r['event_id'] for r in rows if r['event_id'] is not None}
assert len(ids) == 15
assert not unreadable, unreadable
print(json.dumps({'committed_rows': len(rows), 'unique_coordinates': len(coords),
                  'distinct_ids': len(ids), 'committed_files': len(committed),
                  'physical_parquet_files': len(physical), 'orphans': sorted(map(str, orphans)),
                  'unreadable': unreadable,
                  'temporary_files': [str(p.relative_to(root)) for p in root.rglob('*')
                                      if p.is_file() and ('_temporary' in p.parts or p.suffix == '.tmp')]}))
