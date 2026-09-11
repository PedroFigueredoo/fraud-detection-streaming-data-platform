"""Run a bounded real-cluster resilience test; preserves all test artifacts."""
import json
import argparse
import subprocess
import tempfile
import time
from pathlib import Path
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]


def run(*args):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=True).stdout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--during-batch', action='store_true')
    fault = parser.parse_args().during_batch
    name = ('fault-' if fault else 'resilience-') + uuid4().hex[:12]
    checkpoint = '/opt/spark/checkpoints/' + name
    output = '/opt/spark/data/streaming/' + name
    gate = ROOT / 'spark/checkpoints' / (name + '-barrier')

    def audit():
        cp = ROOT / 'spark/checkpoints' / name
        sink = ROOT / 'data/streaming' / name
        snapshot = {}
        for sub in ('offsets', 'commits'):
            snapshot[sub] = {p.name: p.read_text() for p in (cp / sub).glob('*') if p.name.isdigit()}
        snapshot['metadata'] = (cp / 'metadata').read_text() if (cp / 'metadata').exists() else None
        snapshot['sink_commits'] = {p.name: p.read_text() for p in (sink / '_spark_metadata').glob('*') if p.name.isdigit()}
        snapshot['physical_files'] = [str(p.relative_to(sink)) for p in sink.rglob('*') if p.is_file()]
        return snapshot
    logdir = Path(tempfile.mkdtemp(prefix='baf-resilience-'))
    print('Test ID:', name, 'logs:', logdir, flush=True)
    run('docker', 'compose', 'exec', '-T', 'kafka', 'kafka-topics',
        '--bootstrap-server', 'kafka:29092', '--create', '--topic', name,
        '--partitions', '3', '--replication-factor', '1')

    def probe(action, value):
        return run('docker', 'compose', 'exec', '-T', '-e',
                   'PYTHONPATH=/opt/airflow/processing/streaming:/opt/airflow/replay',
                   'airflow-webserver', 'python', '/opt/airflow/project-tests/resilience_probe.py',
                   action, name, str(value))

    def start(log, available=False):
        args = ['bash', 'scripts/run_streaming_job.sh', '--topic', name,
                '--output', output, '--checkpoint', checkpoint]
        if available:
            args.append('--available-now')
        if fault:
            args.extend(['--test-block-file', '/opt/spark/checkpoints/' + gate.name])
        return subprocess.Popen(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)

    def kill_driver():
        code = """import os, signal
from pathlib import Path
matches = []
for p in Path('/proc').iterdir():
    if not p.name.isdigit():
        continue
    try:
        args = (p / 'cmdline').read_bytes().split(b'\\0')
    except (FileNotFoundError, PermissionError):
        continue
    if b'org.apache.spark.deploy.SparkSubmit' in args and CHECKPOINT.encode() in args:
        matches.append(int(p.name))
assert len(matches) == 1, matches
os.kill(matches[0], signal.SIGKILL)
print('SIGKILL driver PID', matches[0])
""".replace('CHECKPOINT', repr(checkpoint))
        return run('docker', 'compose', 'exec', '-T', 'spark-master', 'python3', '-c', code)

    with (logdir / 'first.log').open('w') as log:
        first = start(log)
        try:
            print(probe('publish', 'first'), flush=True)
            deadline = time.monotonic() + 180
            while True:
                if first.poll() is not None:
                    raise RuntimeError('Consumer exited; inspect ' + str(logdir))
                commits = ROOT / 'spark/checkpoints' / name / 'commits'
                if fault and gate.with_suffix('.entered').exists():
                    before = audit()
                    assert '0' in before['offsets'] and not before['commits']
                    assert not before['sink_commits']
                    (logdir / 'before.json').write_text(json.dumps(before, indent=2))
                    print('Executor barrier entered; offsets/0 exists; no checkpoint or sink commit', flush=True)
                    break
                if commits.exists() and any(p.name.isdigit() for p in commits.iterdir()):
                    try:
                        print(probe('check', 16), flush=True)
                        break
                    except subprocess.CalledProcessError:
                        pass
                if time.monotonic() > deadline:
                    raise TimeoutError('First batch did not commit')
                time.sleep(3)
        finally:
            if first.poll() is None:
                print(kill_driver(), flush=True)
            first.wait(timeout=30)
    if fault:
        # Separate test-control file, never a checkpoint edit.
        gate.with_suffix('.release').touch()
        print('Consumer killed during batch 0; barrier released for identical restart', flush=True)
    else:
        print('Consumer interrupted after committed partial workload', flush=True)
        print(probe('publish', 'second'), flush=True)
    with (logdir / 'restart.log').open('w') as log:
        second = start(log, available=True)
        try:
            rc = second.wait(timeout=180)
            assert rc == 0, ('Restart failed', rc, str(logdir))
        finally:
            if second.poll() is None:
                print(kill_driver(), flush=True)
                second.wait(timeout=30)
    print(probe('check', 16 if fault else 21), flush=True)
    if fault:
        after = audit()
        assert after['metadata'] == before['metadata']
        assert after['offsets']['0'] == before['offsets']['0']
        assert '0' in after['commits'] and '0' in after['sink_commits']
        (logdir / 'after.json').write_text(json.dumps(after, indent=2))
        print(run('docker', 'compose', 'exec', '-T', 'airflow-webserver', 'python',
                  '/opt/airflow/project-tests/audit_parquet_sink.py', name), flush=True)
    print(json.dumps({'test_id': name, 'checkpoint': checkpoint, 'output': output,
                      'result': 'PASS', 'logs': str(logdir)}), flush=True)


if __name__ == '__main__':
    main()
