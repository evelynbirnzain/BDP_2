import os
import time
import logging
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logging.basicConfig(level=logging.INFO,
                    handlers=[logging.StreamHandler(), logging.FileHandler('logs/batchingestmanager.log')])


class EventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            logging.info(f"New tenant directory added to staging directory: {event.src_path}")
        else:
            logging.info(f"New file uploaded: {event.src_path}")
            tenant = event.src_path.split('/')[-2]
            executable = f"batchingestapps/{tenant}/batchingest.py"
            if os.path.exists(executable):
                logging.info(f"Executing {executable}")
                subprocess.run(["venv\\Scripts\\python", executable, event.src_path])


if __name__ == "__main__":
    path = '/data/client-staging-input-directory'
    logging.info(f"Watching {path}")

    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(5)
    finally:
        observer.stop()
        observer.join()
