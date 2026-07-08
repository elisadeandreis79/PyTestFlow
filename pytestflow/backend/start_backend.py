import asyncio
import os
import threading
import traceback
import webbrowser
from importlib import resources
from bottle import Bottle, response, run, static_file, route, abort
from pathlib import Path
from pytestflow.backend.websocket_gateway import start_server
from pytestflow.backend.report_manager import report_manager
from pytestflow.config.config_manager import ConfigManager

CONFIG = ConfigManager().get_config()

# --------------------
# Paths
# --------------------

# BASE_DIR = directory di questo file backend/start_backend.py
BASE_DIR = Path(__file__).parent.resolve()

# percorso relativo al frontend
FRONTEND_DIR = (BASE_DIR / "frontend").resolve()

# verifica semplice (opzionale)
if not (FRONTEND_DIR / "index.html").exists():
    raise RuntimeError(f"Frontend not found in {FRONTEND_DIR}")


# --------------------
# HTTP Server (Bottle)
# --------------------

app = Bottle()


@app.route("/")
def index():
    return static_file("index.html", root=FRONTEND_DIR)


@app.route("/assets/<filepath:path>")
def assets(filepath):
    return static_file(filepath, root=os.path.join(FRONTEND_DIR, "assets"))


@app.route("/config.json")
def config():
    response.content_type = 'application/json'
    return {
        "wsPort": CONFIG["websocket"]["port"],
        "featureFlags": {
            "reports": True
        }
    }


# SPA fallback (Vue/React router)
@app.route("/<filepath:path>")
def spa_fallback(filepath):
    return static_file("index.html", root=FRONTEND_DIR)


def start_http():
    run(
        app,
        host=CONFIG["http"]["host"],
        port=CONFIG["http"]["port"],
        quiet=True,
        reloader=False,
    )


# --------------------
# WebSocket Server
# --------------------

def start_ws():
    asyncio.run(start_server(ws_host=CONFIG["websocket"]["host"], ws_port=CONFIG["websocket"]["port"]))


# --------------------
# Main
# --------------------

def main(open_browser=False):
    gui_url = f"http://{CONFIG["http"]["host"]}:{CONFIG["http"]["port"]}/"
    ws_url = f"ws://{CONFIG["websocket"]["host"]}:{CONFIG["websocket"]["port"]}"
    starter_path = os.path.normpath(
        os.path.join(os.path.dirname(BASE_DIR), "starter_here.md")
    )

    print("\nPyTestFlow backend started")
    print(f"GUI URL: {gui_url}")
    print(f"WebSocket URL: {ws_url}")
    print("Next: open the GUI URL, select and launch motherboard test sequence.")
    print(f"Guide: {starter_path}\n")

    if open_browser:
        webbrowser.open(gui_url)

    # Start WebSocket server in background thread (wrap to surface exceptions)
    def _ws_thread_target():
        try:
            start_ws()
        except Exception:
            print("WebSocket thread exception:")
            traceback.print_exc()

    ws_thread = threading.Thread(target=_ws_thread_target, daemon=True)
    ws_thread.start()

    # Start HTTP server (blocking)
    start_http()


if __name__ == "__main__":
    main()
