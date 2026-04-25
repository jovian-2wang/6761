import glob

import os
import socket

import pandas as pd
import matplotlib.pyplot as plt

csv_files = glob.glob("/users/Jiangwei/6761/*_cpu.csv")
if not csv_files:
    raise FileNotFoundError("No *_cpu.csv file found in /users/Jiangwei/6761")

csv_file = csv_files[0]
host = socket.gethostname()

df = pd.read_csv(csv_file)
df["timestamp"] = pd.to_datetime(df["timestamp"])

plt.figure(figsize=(10, 4))
plt.plot(df["timestamp"], df["cpu_percent"], linewidth=1)
plt.xlabel("Time")
plt.ylabel("CPU Utilization (%)")
plt.title(f"CPU Utilization vs. Time - {host}")
plt.xticks(rotation=30)
plt.tight_layout()

out_png = f"/users/Jiangwei/6761/{host}_cpu_plot.png"
plt.savefig(out_png, dpi=200)
plt.close()

print(f"Read: {csv_file}")
print(f"Saved: {out_png}")
