"""Run a bounded real-cluster resilience test; preserves all test artifacts."""
import json
import subprocess
import tempfile
import time
from pathlib import Path
from uuid import uuid4


ROOT = Path(__file__).resolve().parents[1]


def run(*args):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True, check=True).stdout


def main():
    name = 'resilience-' + uuid4().hex[:12]
    checkpoint = '/opt/spark/checkpoints/' + name
    output = '/opt/spark/data/streaming/' + name
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
    print(probe('check', 21), flush=True)
    print(json.dumps({'test_id': name, 'checkpoint': checkpoint, 'output': output,
                      'result': 'PASS', 'logs': str(logdir)}), flush=True)


if __name__ == '__main__':
    main()
