"""Check worker status on Linux machine."""
import paramiko
import time
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('10.100.102.20', username='yadmin', password='0811', timeout=15)

# Check log line count
stdin, stdout, stderr = ssh.exec_command('wc -l /home/yadmin/Desktop/Nexus-Orchestrator/worker.log')
print('Log lines:', stdout.read().decode().strip())

# Check ARQ keys from worker side
stdin, stdout, stderr = ssh.exec_command("redis-cli -h 10.100.102.8 -p 6379 keys 'arq:*'")
print('ARQ keys from worker:', stdout.read().decode().strip())

# Check if worker process is alive
stdin, stdout, stderr = ssh.exec_command('ps aux | grep start_worker | grep -v grep')
print('Worker process:', stdout.read().decode().strip())

# Wait 5s and check log again
time.sleep(5)
stdin, stdout, stderr = ssh.exec_command('wc -l /home/yadmin/Desktop/Nexus-Orchestrator/worker.log')
print('Log lines after 5s:', stdout.read().decode().strip())

# Get last 10 lines of log
stdin, stdout, stderr = ssh.exec_command('tail -10 /home/yadmin/Desktop/Nexus-Orchestrator/worker.log')
print('Last log lines:')
print(stdout.read().decode(errors='replace'))

# Check if there's a .env on the worker
stdin, stdout, stderr = ssh.exec_command('cat /home/yadmin/Desktop/Nexus-Orchestrator/.env 2>/dev/null | grep -E "REDIS|NODE_ROLE"')
print('Worker .env REDIS/NODE_ROLE:')
print(stdout.read().decode(errors='replace'))

ssh.close()
print('Done.')
