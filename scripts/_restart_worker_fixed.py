"""Upload fixed start_worker.py and restart worker."""
import paramiko
import time
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('10.100.102.20', username='yadmin', password='0811', timeout=15)

# Kill existing workers
print('=== Killing existing workers ===')
stdin, stdout, stderr = ssh.exec_command('pkill -f start_worker.py; sleep 2; echo done')
print(stdout.read().decode().strip())

# Upload the fixed start_worker.py
print('=== Uploading fixed start_worker.py ===')
sftp = ssh.open_sftp()
sftp.put(
    'C:/Users/Yarin/Desktop/Nexus-Orchestrator/scripts/start_worker.py',
    '/home/yadmin/Desktop/Nexus-Orchestrator/scripts/start_worker.py'
)
sftp.close()
print('Uploaded.')

# Start worker
print('=== Starting worker ===')
cmd = (
    'cd /home/yadmin/Desktop/Nexus-Orchestrator && '
    'nohup /home/yadmin/Desktop/Nexus-Orchestrator/.venv/bin/python3 '
    'scripts/start_worker.py > worker.log 2>&1 &'
)
stdin, stdout, stderr = ssh.exec_command(cmd)
time.sleep(15)

print('=== Worker log ===')
stdin, stdout, stderr = ssh.exec_command('cat /home/yadmin/Desktop/Nexus-Orchestrator/worker.log')
print(stdout.read().decode(errors='replace'))

print('=== Process ===')
stdin, stdout, stderr = ssh.exec_command('ps aux | grep start_worker | grep -v grep')
print(stdout.read().decode().strip())

print('=== ARQ keys in Redis ===')
stdin, stdout, stderr = ssh.exec_command("redis-cli -h 10.100.102.8 -p 6379 keys 'arq:health*'")
print(stdout.read().decode().strip())

ssh.close()
print('Done.')
