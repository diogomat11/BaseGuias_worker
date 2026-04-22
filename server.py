from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import os
import sys
import psutil
from contextlib import asynccontextmanager

# Add current directory to path so we can import ImportBaseGuias
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# Add parent directory for backend imports
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
# Fix for noconsole mode where stdout/stderr are None
class FileLogStream:
    def __init__(self, filename):
        self.filename = filename
        try: self.log_file = open(filename, "a", encoding="utf-8")
        except: self.log_file = None
    def write(self, data):
        try:
            if self.log_file:
                    self.log_file.write(data)
                    self.log_file.flush()
        except: pass
    def flush(self):
        try: 
            if self.log_file: self.log_file.flush()
        except: pass
    def isatty(self): return False

# Logs will be initialized later when port is known to avoid file conflicts
# if sys.stdout is None: sys.stdout = FileLogStream("server_debug.log")
# if sys.stderr is None: sys.stderr = FileLogStream("server_err.log")

from ImportBaseGuias import UnimedScraper

import threading
import time
from datetime import datetime, timedelta

app = FastAPI()
scraper = None
last_activity_time = datetime.now()
driver_lock = threading.Lock()
INACTIVITY_LIMIT = timedelta(minutes=20)

def kill_orphan_chrome_processes():
    """Kill chrome/chromedriver processes spawned by automation (not the user's browser)."""
    try:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                name = (proc.info.get('name') or '').lower()
                if 'chromedriver' in name:
                    proc.kill()
                elif 'chrome' in name:
                    # Only kill automation chrome instances (spawned by webdriver)
                    cmdline = ' '.join(proc.info.get('cmdline') or [])
                    if '--test-type=webdriver' in cmdline:
                        proc.kill()
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                pass
    except: pass


def maintain_driver_lifecycle():
    global scraper, last_activity_time
    while True:
        time.sleep(60) # Check every minute
        with driver_lock:
            if scraper and scraper.driver:
                if datetime.now() - last_activity_time > INACTIVITY_LIMIT:
                    print(">>> Inactivity limit reached. Closing driver and killing chrome processes.")
                    try:
                        scraper.close_driver()
                        scraper.driver = None # Mark as closed
                    except Exception as e:
                        print(f"Error closing driver: {e}")
                    # Kill ALL orphan chrome/chromedriver processes
                    kill_orphan_chrome_processes()

# Start background thread
threading.Thread(target=maintain_driver_lifecycle, daemon=True).start()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global scraper, last_activity_time
    scraper = UnimedScraper()
    # Initial Start
    with driver_lock:
        scraper.start_driver()
        scraper.login()
        last_activity_time = datetime.now()
    yield
    with driver_lock:
        scraper.close_driver()

app = FastAPI(lifespan=lifespan)

class JobRequest(BaseModel):
    job_id: int
    carteirinha_id: int
    carteirinha: str
    paciente: str = ""


@app.get("/")
async def health_check():
    is_busy = driver_lock.locked()
    return {"status": "ok", "busy": is_busy, "driver_alive": (scraper is not None and scraper.driver is not None)}

@app.post("/restart")
def restart_driver():
    global scraper
    print(">>> Received manual restart request. Closing driver...")
    with driver_lock:
        if scraper:
            try:
                scraper.close_driver()
            except: pass
            # Kill orphan chrome/chromedriver processes for a clean slate
            kill_orphan_chrome_processes()
            time.sleep(1)  # Wait for processes to fully terminate
            try:
                scraper.start_driver()
                scraper.login()
            except Exception as e:
                return {"status": "error", "message": f"Failed to restart: {e}"}
    return {"status": "success", "message": "Driver restarted"}

@app.post("/process_job")
def process_job(job: JobRequest):
    print(f">>> Received Job {job.job_id} for Carteirinha {job.carteirinha}")
    global scraper, last_activity_time
    
    if not scraper:
         raise HTTPException(status_code=503, detail="Scraper not initialized")

    with driver_lock:
        # Check if driver is alive/open
        if not scraper.driver:
            print(">>> Driver is closed (timeout or crash). Restarting...")
            try:
                scraper.start_driver()
                scraper.login()
            except Exception as e:
                return {"status": "error", "message": f"Failed to restart driver: {e}", "carteirinha_id": job.carteirinha_id}
        
        # Check if we should re-login? (Maybe blindly trust it works, if it fails scraping will catch)
        # We assume if it was idle < 20 mins, it's fine. If > 20 mins it was closed.
        
        last_activity_time = datetime.now()

    # Process
    try:
        # Scraper methods might need to be thread-safe if we had parallel requests, 
        # but here we likely have 1 request per worker at a time via dispatcher.
        # But we holding lock? No, scraping takes time. We shouldn't hold lock during scraping
        # if we want other status checks (health) to work, but for now single thread logic is safer.
        # Ideally we release lock, but safeguard 'scraper' instance. 
        # Since scraper.driver is shared, we should probably keep lock if scraping modifies driver state? 
        # Selenium is not thread safe. So yes, hold lock or ensure serial execution.
        
        with driver_lock:
             # Double check existence
             if not scraper.driver:
                  raise Exception("Driver died unexpectedly before scraping.")
             
             results = scraper.process_carteirinha(
                job.carteirinha, 
                job_id=job.job_id, 
                carteirinha_db_id=job.carteirinha_id
             )
             last_activity_time = datetime.now()
             print(f">>> Returning {len(results)} items for Job {job.job_id}")
             
        return {"status": "success", "data": results, "carteirinha_id": job.carteirinha_id}
    except Exception as e:
        # Log critical failure to DB using a fresh session
        from database import SessionLocal
        from models import Log
        db = SessionLocal()
        try:
            db.add(Log(job_id=job.job_id, carteirinha_id=job.carteirinha_id, level="ERROR", message=f"Server Crash: {str(e)}"))
            db.commit()
        except Exception as log_e:
            print(f"Failed to log server crash: {log_e}")
            try: db.rollback()
            except: pass
        finally:
            db.close()

        # CRITICAL: Reset driver state on failure to avoid "broken" sessions for next jobs
        print(f">>> ERROR during job processing: {e}. Resetting driver for next attempt.")
        with driver_lock:
            try:
                if scraper:
                    scraper.close_driver()
                    scraper.driver = None
                kill_orphan_chrome_processes()
            except: pass

        return {"status": "error", "message": str(e), "carteirinha_id": job.carteirinha_id}



class QueueLogger:
    def __init__(self, queue, prefix="Worker"):
        self.queue = queue
        self.prefix = prefix
    def write(self, message):
        if message.strip():
            self.queue.put(f"[{self.prefix}] {message.strip()}")
    def flush(self):
        pass
    def isatty(self):
        return False

def run_server(port=8000, log_queue=None):
    if log_queue:
        sys.stdout = QueueLogger(log_queue, f"Worker-{port}")
        sys.stderr = QueueLogger(log_queue, f"Worker-{port} ERR")
    else:
        # Port-specific log files for debugging
        sys.stdout = FileLogStream(f"server_{port}_debug.log")
        sys.stderr = FileLogStream(f"server_{port}_err.log")
        
    uvicorn.run(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    # Port will be passed via arg or env, default 8000
    port = int(os.environ.get("PORT", 8010))
    run_server(port)
