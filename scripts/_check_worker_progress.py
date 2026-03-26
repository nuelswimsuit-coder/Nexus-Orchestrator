"""Check worker progress."""
import paramiko
import time
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('10.100.102.20', username='yadmin', password='0811', timeout=15)

time.sleep(15)

print('=== Worker log tail ===')
stdin, stdout, stderr = ssh.exec_command('tail -15 /home/yadmin/Desktop/Nexus-Orchestrator/worker.log')
print(stdout.read().decode(errors='replace'))

print('=== Process ===')
stdin, stdout, stderr = ssh.exec_command('ps aux | grep start_worker | grep -v grep')
print(stdout.read().decode().strip())

print('=== ARQ health keys ===')
stdin, stdout, stderr = ssh.exec_command('redis-cli -h 10.100.102.8 -p 6379 keys arq:health-check')
print(stdout.read().decode().strip())

print('=== nexus:tasks:health-check ===')
stdin, stdout, stderr = ssh.exec_command('redis-cli -h 10.100.102.8 -p 6379 get nexus:tasks:health-check')
print(stdout.read().decode().strip())

ssh.close()
print('Done.')
