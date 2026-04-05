import pandas as pd
import matplotlib.pyplot as plt

files = {
    "node-0": "/users/Jiangwei/6761/cpu_node0.csv",
    "node-1": "/users/Jiangwei/6761/cpu_node1.csv",
    "node-2": "/users/Jiangwei/6761/cpu_node2.csv",
}

for label, path in files.items():
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["cpu_used"] = pd.to_numeric(df["cpu_used"], errors="coerce")
    plt.figure(figsize=(10, 5))
    plt.plot(df["timestamp"], df["cpu_used"])
    plt.xlabel("Time")
    plt.ylabel("CPU Utilization (%)")
    plt.title(f"CPU Utilization vs Time - {label}")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"/users/Jiangwei/6761/{label}_cpu_plot.png")
    plt.close()
