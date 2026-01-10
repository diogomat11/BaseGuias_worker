import sys
import os
import time
import requests
import logging
from datetime import datetime, timedelta
# Add parent dir to path to import backend modules
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from backend.database import SessionLocal
from backend.models import Job, BaseGuia

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERVERS = [url.strip() for url in os.environ.get("API_SERVER_URLS", "http://127.0.0.1:8000").split(",")]
SERVER_STATUS = {url: {"status": "idle", "last_job": None} for url in SERVERS}
DISPATCH_STAGGER = int(os.environ.get("DISPATCH_STAGGER_SECONDS", 15))

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def check_stuck_jobs(db):
    # logic to release jobs that are processing for too long or server crashed?
    # User said: "JOB com status(pending, error[caso data update_at do jobtenha mais de 5minutos ])"
    # So we pick up errors > 5 mins too?
    pass

def get_pending_job(db):
    # Priority: pending, or error > 5 mins
    # Sort by priority desc, created_at asc
    
    five_min_ago = datetime.utcnow() - timedelta(minutes=5)
    
    # Try pending first
    job = db.query(Job).filter(Job.status == "pending").order_by(Job.priority.desc(), Job.created_at.asc()).first()
    if job:
        return job
        
    # Try error > 5 mins and attempts < 5
    job = db.query(Job).filter(
        Job.status == "error", 
        Job.updated_at < five_min_ago,
        Job.attempts < 5
    ).order_by(Job.priority.desc(), Job.created_at.asc()).first()
    
    return job

def dispatch():
    logger.info("Starting Dispatcher...")
    while True:
        try:
            db = SessionLocal()
            
            # 1. Check available servers
            available_servers = [url for url, meta in SERVER_STATUS.items() if meta["status"] == "idle"]
            
            if not available_servers:
                logger.info("No servers available. Waiting...")
            else:
                for server_url in available_servers:
                    # Get Job
                    job = get_pending_job(db)
                    if not job:
                        logger.info("No pending jobs.")
                        break
                    
                    logger.info(f"Assigning Job {job.id} to {server_url}")
                    
                    # Lock Job
                    job.status = "processing"
                    job.locked_by = server_url
                    job.attempts += 1
                    job.updated_at = datetime.utcnow()
                    db.commit()
                    
                    # Update Local Server Status (Mocking async dispatch for now)
                    SERVER_STATUS[server_url]["status"] = "busy"
                    SERVER_STATUS[server_url]["last_job"] = job.id
                    
                    # Call Server (Blocking for simplicity in this MVP, but ideally async)
                    # To respect "Avoid concurrency immediate", maybe we sleep here?
                    # But if we block, we effectively limit throughput.
                    # The requirement says "independent servers" running script.
                    # We can use threading to make the request non-blocking.
                    
                    import threading
                    def call_server(url, job_id, carteirinha, carteirinha_id):
                        try:
                            payload = {
                                "job_id": job_id,
                                "carteirinha_id": carteirinha_id,
                                "carteirinha": carteirinha,
                                "paciente": "" 
                            }
                            # Log attempt
                            try:
                                temp_log_session = SessionLocal()
                                from backend.models import Log
                                temp_log_session.add(Log(job_id=job_id, carteirinha_id=carteirinha_id, level="INFO", message=f"Dispatching to {url}"))
                                temp_log_session.commit()
                                temp_log_session.close()
                            except: pass

                            
                            resp = requests.post(f"{url}/process_job", json=payload, timeout=300)
                            
                            try:
                                data = resp.json()
                            except ValueError: 
                                # JSONDecodeError
                                err_msg = f"Invalid JSON ({resp.status_code}): {resp.text[:200]}"
                                thread_db.add(Log(job_id=job_id, carteirinha_id=carteirinha_id, level="ERROR", message=f"Worker Protocol Error: {err_msg}"))
                                thread_db.commit()
                                raise Exception(err_msg)

                            # New DB Session for thread
                            thread_db = SessionLocal()
                            current_job = thread_db.query(Job).filter(Job.id == job_id).first()
                            
                            if data.get("status") == "success":
                                current_job.status = "success"
                                results = data.get("data", [])
                                # Save results to BaseGuia
                                try:
                                    count_inserted = 0
                                    count_updated = 0
                                    
                                    def parse_date(date_str):
                                        if not date_str or not isinstance(date_str, str):
                                            return None
                                        try:
                                            return datetime.strptime(date_str.strip(), "%d/%m/%Y").date()
                                        except:
                                            return None

                                    logger.info(f"Processing {len(results)} items from worker response.")
                                    for item in results:
                                        logger.info(f"Processing item: {item}")
                                        # Map scraper keys to BaseGuia columns
                                        # Conversion helpers
                                        try:
                                            qtd_solic_val = int(item.get("qtde_solicitada"))
                                        except:
                                            qtd_solic_val = 0
                                            
                                        try:
                                            qtd_aut_val = int(item.get("qtde_autorizada"))
                                        except:
                                            qtd_aut_val = 0
                                            
                                        guia_num = item.get("numero_guia")
                                        
                                        data_auth_parsed = parse_date(item.get("data_autorizacao"))
                                        validade_parsed = parse_date(item.get("validade_senha"))
                                        
                                        logger.info(f"Parsed values - Guia: {guia_num}, DatAuth: {data_auth_parsed}, Val: {validade_parsed}, QtdSol: {qtd_solic_val}, QtdAut: {qtd_aut_val}")
                                        
                                        # UPSERT Logic: Check if exists
                                        logger.info("Checking if guia exists in DB...")
                                        try:
                                            existing_guia = thread_db.query(BaseGuia).filter(
                                                BaseGuia.carteirinha_id == carteirinha_id,
                                                BaseGuia.guia == guia_num
                                            ).first()
                                            logger.info(f"DB Query result: {existing_guia}")
                                        except Exception as db_q_err:
                                            logger.error(f"DB Query Failed: {db_q_err}")
                                            raise db_q_err
                                        
                                        if existing_guia:
                                            # Update
                                            logger.info("Updating existing guia...")
                                            existing_guia.data_autorizacao = data_auth_parsed
                                            existing_guia.senha = item.get("senha")
                                            existing_guia.validade = validade_parsed
                                            existing_guia.codigo_terapia = item.get("codigo_terapia")
                                            existing_guia.qtde_solicitada = qtd_solic_val
                                            existing_guia.sessoes_autorizadas = qtd_aut_val
                                            existing_guia.updated_at = datetime.utcnow()
                                            count_updated += 1
                                        else:
                                            # Insert
                                            logger.info(f"Inserting new guia: {guia_num}")
                                            try:
                                                new_guia = BaseGuia(
                                                    carteirinha_id=carteirinha_id,
                                                    guia=guia_num,
                                                    data_autorizacao=data_auth_parsed,
                                                    senha=item.get("senha"),
                                                    validade=validade_parsed,
                                                    codigo_terapia=item.get("codigo_terapia"),
                                                    qtde_solicitada=qtd_solic_val,
                                                    sessoes_autorizadas=qtd_aut_val,
                                                    created_at=datetime.utcnow()
                                                )
                                                logger.info("BaseGuia object created successfully")
                                                thread_db.add(new_guia)
                                                logger.info("Added to session, incrementing count")
                                                count_inserted += 1
                                            except Exception as insert_err:
                                                logger.error(f"Failed to create/add BaseGuia: {insert_err}")
                                                raise insert_err
                                    
                                    # Explicit Commit Log
                                    logger.info("Committing changes to DB...")
                                    thread_db.commit()
                                    logger.info("Commit successful.")
                                    
                                    # Create fresh session for final log
                                    log_session = SessionLocal()
                                    log_session.add(Log(job_id=job_id, carteirinha_id=carteirinha_id, level="INFO", message=f"Sync complete. Inserted: {count_inserted}, Updated: {count_updated}"))
                                    log_session.commit()
                                    log_session.close()
                                except Exception as save_e:
                                    logger.error(f"Exception during save: {save_e}")
                                    # Create fresh session for error log
                                    err_log_session = SessionLocal()
                                    err_log_session.add(Log(job_id=job_id, carteirinha_id=carteirinha_id, level="ERROR", message=f"Error saving results: {save_e}"))
                                    err_log_session.commit()
                                    err_log_session.close()
                                    current_job.status = "error"
                            else:
                                current_job.status = "error"
                                # Log error from server
                                err_msg = data.get("message") or data.get("detail") or "Unknown error from server"
                                thread_db.add(Log(job_id=job_id, carteirinha_id=carteirinha_id, level="ERROR", message=f"Worker Error: {err_msg}"))
                            
                            current_job.locked_by = None
                            current_job.updated_at = datetime.utcnow()
                            thread_db.commit()
                            thread_db.close()
                            
                        except Exception as e:
                            logger.error(f"Error calling server {url}: {e}")
                            thread_db = SessionLocal()
                            current_job = thread_db.query(Job).filter(Job.id == job_id).first()
                            if current_job:
                                current_job.status = "error"
                                current_job.locked_by = None
                                current_job.updated_at = datetime.utcnow()
                            
                                # Log dispatcher error
                                try:
                                    thread_db.add(Log(job_id=job_id, carteirinha_id=carteirinha_id, level="ERROR", message=f"Dispatcher Failed: {str(e)}"))
                                except: pass
                                
                                thread_db.commit()
                            thread_db.close()
                            
                        finally:
                            SERVER_STATUS[url]["status"] = "idle"

                    # Fetch carteirinha
                    cart_obj = job.carteirinha_rel
                    t = threading.Thread(target=call_server, args=(server_url, job.id, cart_obj.carteirinha, cart_obj.id))
                    t.start()
                    
                    # Stagger
                    time.sleep(DISPATCH_STAGGER) 
            
            db.close()
            time.sleep(DISPATCH_STAGGER)
            
        except Exception as e:
            logger.error(f"Dispatcher Loop Error: {e}")
            time.sleep(15)

if __name__ == "__main__":
    dispatch()
