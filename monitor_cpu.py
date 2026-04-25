import csv
import socket
import time
from datetime import datetime

HOST = socket.gethostname()
OUT = f"/users/Jiangwei/6761/{HOST}_cpu.csv"
DURATION_SECONDS = 25 * 60
INTERVAL_SECONDS = 2

def read_cpu_times():
    with open("/proc/stat", "r") as f:
        parts = f.readline().split()[1:]
    values = list(map(int, parts))
    idle = values[3] + values[4]
    total = sum(values)
    return idle, total

idle_prev, total_prev = read_cpu_times()

with open(OUT, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "cpu_percent"])

    for _ in range(DURATION_SECONDS // INTERVAL_SECONDS):
        time.sleep(INTERVAL_SECONDS)
        idle_now, total_now = read_cpu_times()

        idle_delta = idle_now - idle_prev
        total_delta = total_now - total_prev

        cpu_percent = 100.0 * (1.0 - idle_delta / total_delta) if total_delta else 0.0

        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            round(cpu_percent, 2)
        ])

        idle_prev, total_prev = idle_now, total_now

print(f"Saved CPU log to {OUT}")
