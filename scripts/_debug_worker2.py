"""Debug: run worker with full stderr capture."""
import paramiko
import time
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('10.100.102.20', username='yadmin', password='0811', timeout=15)

# Kill any existing worker
ssh.exec_command('pkill -f start_worker.py 2>/dev/null; sleep 1')
time.sleep(2)

# Run worker with stderr to a separate file
cmd = (
    'cd /home/yadmin/Desktop/Nexus-Orchestrator && '
    '/home/yadmin/Desktop/Nexus-Orchestrator/.venv/bin/python3 '
    'scripts/start_worker.py > /tmp/worker_stdout.log 2> /tmp/worker_stderr.log &'
)
ssh.exec_command(cmd)
time.sleep(8)

print('=== STDOUT ===')
stdin, stdout, stderr = ssh.exec_command('cat /tmp/worker_stdout.log')
print(stdout.read().decode(errors='replace'))

print('=== STDERR ===')
stdin, stdout, stderr = ssh.exec_command('cat /tmp/worker_stderr.log')
print(stdout.read().decode(errors='replace'))

print('=== WORKER LOG ===')
stdin, stdout, stderr = ssh.exec_command('cat /home/yadmin/Desktop/Nexus-Orchestrator/worker.log 2>/dev/null')
print(stdout.read().decode(errors='replace'))

print('=== PROCESS ===')
stdin, stdout, stderr = ssh.exec_command('ps aux | grep start_worker | grep -v grep')
print(stdout.read().decode().strip())

ssh.close()
print('Done.')
