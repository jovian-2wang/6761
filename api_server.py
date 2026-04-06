import web
import os
import csv
import json
import subprocess

urls = (
    '/addSource', 'AddSource',
    '/listSources', 'ListSources',
    '/count', 'Count',
    '/(.*)', 'NotFound'
)

app = web.application(urls, globals())

STATE_FILE = "/users/Jiangwei/6761/api_state.json"
BASE_OUTPUT = "/users/Jiangwei/6761/output"
ALLOWED_SOURCES = ["idigbio", "gbif", "obis"]

BROKERS = [
    "node-0.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092",
    "node-1.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092",
    "node-2.project-jw.ufl-eel6761-sp26-pg0.wisc.cloudlab.us:9092",
]

SOURCE_SCRIPT = {
    "idigbio": "/users/Jiangwei/6761/stream-idigbio.py",
    "gbif": "/users/Jiangwei/6761/stream-gbif.py",
    "obis": "/users/Jiangwei/6761/stream-obis.py",
}



def load_state():
    if not os.path.exists(STATE_FILE):
        return {
            "active_sources": [],
            "next_broker_idx": 0,
            "source_to_broker": {},
            "source_to_pid": {}
        }
    with open(STATE_FILE, "r") as f:
        return json.load(f)

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def latest_csv_file(folder):
    if not os.path.isdir(folder):
        return None
    files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.endswith(".csv")
    ]
    if not files:
        return None
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]

class AddSource:
    def GET(self):
        params = web.input(name=None)
        name = params.name

        if name not in ALLOWED_SOURCES:
            raise web.badrequest()

        state = load_state()

        if name in state["active_sources"]:
            web.header("Content-Type", "text/plain")
            return f"{name} already active"

        broker_idx = state["next_broker_idx"]
        broker = BROKERS[broker_idx]
        script = SOURCE_SCRIPT[name]

        env = os.environ.copy()
        env["KAFKA_BOOTSTRAP_SERVERS"] = broker

        out_log = open(f"/users/Jiangwei/6761/{name}.log", "a")
        err_log = open(f"/users/Jiangwei/6761/{name}.err", "a")

        proc = subprocess.Popen(
            ["python3", script],
            env=env,
            stdout=out_log,
            stderr=err_log
        )

        state["active_sources"].append(name)
        state["source_to_broker"][name] = broker
        state["source_to_pid"][name] = proc.pid
        state["next_broker_idx"] = (broker_idx + 1) % len(BROKERS)

        save_state(state)

        web.header("Content-Type", "text/plain")
        return f"started {name} on {broker} with pid {proc.pid}"


class ListSources:
    def GET(self):
        state = load_state()
        web.header("Content-Type", "text/plain")
        return "\n".join(state["active_sources"])

class Count:
    def GET(self):
        params = web.input(by=None)
        by = params.by

        if by not in ["source", "kingdom", "totalSpecies"]:
            raise web.badrequest()

        web.header("Content-Type", "text/plain")

        if by == "source":
            path = latest_csv_file(f"{BASE_OUTPUT}/source_counts")
            if not path:
                return ""
            lines = []
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    lines.append(f"{row['source']} {row['record_count']}")
            return "\n".join(lines)

        elif by == "kingdom":
            path = latest_csv_file(f"{BASE_OUTPUT}/kingdom_counts")
            if not path:
                return ""
            lines = []
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    lines.append(f"plantae {row['plant_count']}")
                    lines.append(f"animalia {row['animal_count']}")
                    lines.append(f"fungi {row['fungi_count']}")
            return "\n".join(lines)

        else:
            path = latest_csv_file(f"{BASE_OUTPUT}/species_counts")
            if not path:
                return ""
            lines = []
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    lines.append(f"species {row['species']}")
            return "\n".join(lines)

class NotFound:
    def GET(self, path):
        raise web.notfound()
    def POST(self, path):
        raise web.notfound()

if __name__ == "__main__":
    app.run()
