"""One-shot: install ccxt on Linux worker and restart the worker process."""
import paramiko
import time
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('10.100.102.20', username='yadmin', password='0811', timeout=15)

NEXUS_DIR = '/home/yadmin/Desktop/Nexus-Orchestrator'

print('=== Finding venv / pip ===')
stdin, stdout, stderr = ssh.exec_command(
    f'ls {NEXUS_DIR}/.venv/bin/pip 2>/dev/null || ls {NEXUS_DIR}/venv/bin/pip 2>/dev/null || which pip3'
)
print(stdout.read().decode().strip())

# Determine pip path
stdin, stdout, stderr = ssh.exec_command(
    f'test -f {NEXUS_DIR}/.venv/bin/pip && echo {NEXUS_DIR}/.venv/bin/pip'
    f' || (test -f {NEXUS_DIR}/venv/bin/pip && echo {NEXUS_DIR}/venv/bin/pip)'
    f' || echo pip3_system'
)
pip_path = stdout.read().decode().strip()
print('Using pip:', pip_path)

if pip_path == 'pip3_system':
    install_cmd = f'pip3 install ccxt --break-system-packages 2>&1'
else:
    install_cmd = f'{pip_path} install ccxt 2>&1'

print('\n=== Installing ccxt ===')
stdin, stdout, stderr = ssh.exec_command(install_cmd)
stdout.channel.settimeout(180)
out = stdout.read().decode(errors='replace')
print(out[-3000:] if len(out) > 3000 else out)

# Determine python path
if pip_path == 'pip3_system':
    py_path = 'python3'
else:
    py_path = pip_path.replace('/pip', '/python3')

print('\n=== Verifying ccxt ===')
stdin, stdout, stderr = ssh.exec_command(f'{py_path} -c "import ccxt; print(ccxt.__version__)"')
print('version:', stdout.read().decode().strip())
print('err:', stderr.read().decode().strip())

print('\n=== Killing old worker ===')
stdin, stdout, stderr = ssh.exec_command('pkill -f start_worker.py; pkill -f nexus_worker; sleep 1; echo killed')
print(stdout.read().decode().strip())

print('\n=== Starting worker ===')
cmd = f'cd {NEXUS_DIR} && nohup {py_path} scripts/start_worker.py > worker.log 2>&1 &'
stdin, stdout, stderr = ssh.exec_command(cmd)
time.sleep(5)

print('\n=== Checking worker started ===')
stdin, stdout, stderr = ssh.exec_command('ps aux | grep start_worker | grep -v grep')
print(stdout.read().decode())

print('\n=== Worker log tail ===')
stdin, stdout, stderr = ssh.exec_command(f'tail -40 {NEXUS_DIR}/worker.log 2>&1')
print(stdout.read().decode(errors='replace'))

ssh.close()
print('Done.')
