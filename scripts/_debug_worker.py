"""Debug: check worker log after waiting 30s."""
import paramiko
import time
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('10.100.102.20', username='yadmin', password='0811', timeout=15)

# Wait and check if log grows
print('=== Log at T+0 ===')
stdin, stdout, stderr = ssh.exec_command('wc -l /home/yadmin/Desktop/Nexus-Orchestrator/worker.log')
print(stdout.read().decode().strip())

time.sleep(10)

print('=== Log at T+10 ===')
stdin, stdout, stderr = ssh.exec_command('wc -l /home/yadmin/Desktop/Nexus-Orchestrator/worker.log')
print(stdout.read().decode().strip())

print('=== Full worker log ===')
stdin, stdout, stderr = ssh.exec_command('cat /home/yadmin/Desktop/Nexus-Orchestrator/worker.log')
print(stdout.read().decode(errors='replace'))

print('=== Worker process ===')
stdin, stdout, stderr = ssh.exec_command('ps aux | grep start_worker | grep -v grep')
print(stdout.read().decode().strip())

# Check if there's a boot notifier hanging
print('=== Network connections from worker ===')
stdin, stdout, stderr = ssh.exec_command('ss -tnp | grep 18725 2>/dev/null || netstat -tnp 2>/dev/null | grep 18725')
print(stdout.read().decode(errors='replace'))

ssh.close()
print('Done.')
