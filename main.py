
import subprocess

# Find all log files in spark work directory
result = subprocess.run(
    ['docker', 'exec', 'spark-master', 'bash', '-c', 
     'find /opt/spark/work -name "stderr" -o -name "stdout" 2>/dev/null | head -20'],
    capture_output=True, text=True
)
print("Log files found:")
print(result.stdout)

# Check the most recent application logs
result = subprocess.run(
    ['docker', 'exec', 'spark-master', 'bash', '-c',
     'for dir in $(ls -t /opt/spark/work/ | head -5); do echo "=== $dir ==="; cat /opt/spark/work/$dir/stderr 2>/dev/null | tail -50; done'],
    capture_output=True, text=True
)
print("Recent application logs:")
print(result.stdout[-10000:])