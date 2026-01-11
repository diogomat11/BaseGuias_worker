from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import os
import sys
from contextlib import asynccontextmanager

# Add current directory to path so we can import ImportBaseGuias
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# Add parent directory for backend imports
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from ImportBaseGuias import UnimedScraper

import threading
import time
from datetime import datetime, timedelta

app = FastAPI()
scraper = None
last_activity_time = datetime.now()
driver_lock = threading.Lock()
INACTIVITY_LIMIT = timedelta(minutes=20)

def maintain_driver_lifecycle():
    global scraper, last_activity_time
    while True:
        time.sleep(60) # Check every minute
        with driver_lock:
            if scraper and scraper.driver:
                if datetime.now() - last_activity_time > INACTIVITY_LIMIT:
                    print(">>> Inactivity limit reached. Closing driver.")
                    try:
                        scraper.close_driver()
                        scraper.driver = None # Mark as closed
                    except Exception as e:
                        print(f"Error closing driver: {e}")

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
        # Log critical failure to DB if scraper didn't catch it
        if scraper and scraper.db:
             try:
                 from models import Log
                 scraper.db.add(Log(job_id=job.job_id, carteirinha_id=job.carteirinha_id, level="ERROR", message=f"Server Crash: {str(e)}"))
                 scraper.db.commit()
             except: pass
        return {"status": "error", "message": str(e), "carteirinha_id": job.carteirinha_id}

if __name__ == "__main__":
    # Port will be passed via arg or env, default 8000
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    # Port will be passed via arg or env, default 8000
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
