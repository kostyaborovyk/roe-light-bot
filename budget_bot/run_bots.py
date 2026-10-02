"""Two private processes in the existing service. Never change BOT_TOKEN."""
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def main():
    light = Path(os.getenv('LIGHT_BOT_PATH', str(ROOT.parent / 'bot.py'))).resolve()
    if not light.is_file():
        raise RuntimeError('LIGHT_BOT_PATH має вказувати на наявний bot.py відключень.')
    if not os.getenv('BUDGET_BOT_TOKEN') or not os.getenv('BOT_TOKEN'):
        raise RuntimeError('Потрібні BOT_TOKEN і BUDGET_BOT_TOKEN у Render Environment.')
    if os.environ['BUDGET_BOT_TOKEN'] == os.environ['BOT_TOKEN']:
        raise RuntimeError('Два боти повинні мати різні токени.')
    stop = threading.Event()
    children = {}
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    def supervise(name, path):
        delay = 2
        while not stop.is_set():
            started = time.monotonic()
            proc = subprocess.Popen([sys.executable, '-u', str(path)], cwd=light.parent,
                                    env=os.environ.copy())
            children[name] = proc
            while proc.poll() is None and not stop.wait(1):
                pass
            if stop.is_set():
                proc.terminate()
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                break
            print(f'{name} exited ({proc.returncode}); restarting in {delay}s', flush=True)
            if time.monotonic() - started > 60:
                delay = 2
            if stop.wait(delay):
                break
            delay = min(delay * 2, 60)

    class Health(BaseHTTPRequestHandler):
        def do_GET(self):
            healthy = all(name in children and children[name].poll() is None for name in ('light', 'budget'))
            self.send_response(200 if healthy else 503)
            self.end_headers()
            self.wfile.write(b'ok' if healthy else b'starting')

        def log_message(self, *_):
            pass

    threads = [threading.Thread(target=supervise, args=(name, path)) for name, path in
               [('light', light), ('budget', ROOT / 'budget_bot.py')]]
    for thread in threads:
        thread.start()
    server = None
    if os.getenv('PORT'):
        server = ThreadingHTTPServer(('0.0.0.0', int(os.environ['PORT'])), Health)
        threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        while not stop.wait(1):
            pass
    finally:
        stop.set()
        for thread in threads:
            thread.join()
        if server:
            server.shutdown()
            server.server_close()


if __name__ == '__main__':
    main()
