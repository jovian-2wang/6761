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

def load_state():
    if not os.path.exists(STATE_FILE):
        return {"active_sources": []}
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
        if name not in state["active_sources"]:
            state["active_sources"].append(name)
            save_state(state)

        return ""

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
