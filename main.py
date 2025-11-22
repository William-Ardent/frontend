# main.py
import os
import time
import uuid
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import requests
from flask import Flask, request, jsonify, send_file, abort
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, JSON, func, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


DATABASE_URL = "postgresql+psycopg2://avnadmin:AVNS_NqssOw97Ohpnze-RhmW@pg-17f74731-jobsscraper.k.aivencloud.com:27894/defaultdb?sslmode=require"

APIFY_TOKEN = "apify_api_5LNsBAXRbGwuLaT7M9K0gZwHfDeOzT1L202a"

APIFY_ACTOR_ID = "umWOjFtkc7Id6qcBp"

HUNTER_API_KEY = "fa610d791c0f465f27b5ee63dbbd492de330f522"

CLEARBIT_AUTOCOMPLETE = "https://autocomplete.clearbit.com/v1/companies/suggest?query={}"

PORT = 8080
WORKER_POOL_SIZE = 4
APIFY_POLL_INTERVAL = 3
APIFY_RUN_TIMEOUT = 300
FRONTEND_PATH = "frontend.html"


PORT = int(os.getenv("PORT", "8080"))
WORKER_POOL_SIZE = int(os.getenv("WORKER_POOL_SIZE", "4"))
APIFY_POLL_INTERVAL = float(os.getenv("APIFY_POLL_INTERVAL", "3"))
APIFY_RUN_TIMEOUT = int(os.getenv("APIFY_RUN_TIMEOUT", "300"))  # seconds
FRONTEND_PATH = os.getenv("FRONTEND_PATH", "frontend.html")

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("job-scraper-backend")

# -------------------------
# Flask app & DB init
# -------------------------
app = Flask(__name__, static_folder=None)
# Always use Aiven DB
logger.info("Using hard-coded DATABASE_URL")


engine = create_engine(DATABASE_URL, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# -------------------------
# DB models
# -------------------------
class Run(Base):
    __tablename__ = "runs"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(String, index=True)  # running, success, failed
    initiated_by = Column(String, nullable=True)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    finished_at = Column(DateTime, nullable=True)
    total = Column(Integer, default=0)
    new_rows = Column(Integer, default=0)
    errors = Column(Integer, default=0)
    meta = Column(JSON, nullable=True)


class Result(Base):
    __tablename__ = "results"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    job_title = Column(String, nullable=True)
    company_name = Column(String, nullable=True)
    company_domain = Column(String, nullable=True)
    company_location = Column(String, nullable=True)
    job_url = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    email = Column(String, nullable=True)
    source = Column(String, nullable=True)
    scraped_at = Column(DateTime, server_default=func.now())
    raw = Column(JSON, nullable=True)
    # REMOVE THIS LINE - constraint already exists
    # __table_args__ = (UniqueConstraint("job_url", name="uq_job_url"),)

# Create tables
Base.metadata.create_all(bind=engine)

# -------------------------
# Background worker pool
# -------------------------
executor = ThreadPoolExecutor(max_workers=WORKER_POOL_SIZE)

# -------------------------
# Helpers: Apify, Clearbit, Hunter
# -------------------------
def run_apify_actor(should_wait: bool = True, run_input: Optional[dict] = None):
    """
    Triggers Apify actor run. Returns the actor run object or run-id/url.
    """
    if not APIFY_TOKEN or not APIFY_ACTOR_ID:
        raise RuntimeError("APIFY_TOKEN and APIFY_ACTOR_ID must be set")

    headers = {"Authorization": f"Bearer {APIFY_TOKEN}", "Content-Type": "application/json"}
    
    # Remove token from URL - use only Bearer authentication
    run_url = f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/runs"
    
    payload = run_input or {}
    logger.info("Triggering Apify actor %s", APIFY_ACTOR_ID)
    
    r = requests.post(run_url, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    run_obj = r.json()
    run_id = run_obj.get("data", {}).get("id") or run_obj.get("id")
    logger.info("Apify actor started: run_id=%s", run_id)

    if not should_wait:
        return run_obj

    # Poll for completion - also remove token from status URL
    start = time.time()
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    
    while True:
        r2 = requests.get(status_url, headers=headers, timeout=30)
        r2.raise_for_status()
        data = r2.json().get("data", {})
        status = data.get("status")
        if status in ("SUCCEEDED", "FAILED", "ABORTED"):
            logger.info("Apify run finished with status=%s", status)
            return data
        if time.time() - start > APIFY_RUN_TIMEOUT:
            raise TimeoutError("Apify run timed out")
        time.sleep(APIFY_POLL_INTERVAL)

def fetch_apify_results_from_run_data(run_data: dict):
    """
    Extract dataset / default key-value store results from Apify run data.
    Tries to fetch dataset items if present.
    """
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}"}
    
    # Apify stores data in defaultDatasetId, not datasetId
    dataset_id = run_data.get("defaultDatasetId")
    if not dataset_id:
        logger.warning("No defaultDatasetId in run_data; returning run_data as single item")
        return [run_data]

    # Use the correct dataset URL
    items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    
    r = requests.get(items_url, headers=headers, timeout=30)
    r.raise_for_status()
    items = r.json()
    logger.info("Fetched %d items from Apify dataset %s", len(items), dataset_id)
    return items


def clearbit_get_domain(company_name: str) -> Optional[str]:
    if not company_name:
        return None
    url = CLEARBIT_AUTOCOMPLETE.format(requests.utils.quote(company_name))
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            logger.warning("Clearbit returned %s for company=%s", r.status_code, company_name)
            return None
        candidates = r.json()
        if not candidates:
            return None
        # choose best candidate (first)
        domain = candidates[0].get("domain")
        logger.info("Clearbit resolved '%s' -> domain=%s", company_name, domain)
        return domain
    except Exception as e:
        logger.exception("Clearbit lookup failed for %s: %s", company_name, e)
        return None

def hunter_get_emails_for_domain(domain: str):
    if not domain or not HUNTER_API_KEY:
        return []
    url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={HUNTER_API_KEY}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            logger.warning("Hunter returned %s for domain=%s", r.status_code, domain)
            return []
        data = r.json().get("data", {})
        emails = [e.get("value") for e in data.get("emails", []) if e.get("value")]
        logger.info("Hunter found %d emails for domain %s", len(emails), domain)
        return emails
    except Exception as e:
        logger.exception("Hunter lookup failed for %s: %s", domain, e)
        return []

# -------------------------
# Core background job
# -------------------------
def process_run(run_id: str, actor_input: dict):
    logger.info("Background job starting run_id=%s", run_id)
    db = SessionLocal()
    try:
        run = db.query(Run).get(run_id)
        if not run:
            logger.error("Run not found in DB: %s", run_id)
            return

        # Trigger Apify
        try:
            apify_data = run_apify_actor(should_wait=True, run_input=actor_input)
        except Exception as e:
            logger.exception("Apify actor failed")
            run.status = "failed"
            run.finished_at = func.now()
            run.meta = {"error": str(e)}
            db.add(run)
            db.commit()
            return

        # fetch results items
        try:
            items = fetch_apify_results_from_run_data(apify_data)
        except Exception as e:
            logger.exception("Failed fetching Apify results")
            items = []

        total = 0
        new_rows = 0
        errors = 0

        for item in items:
            total += 1
            try:
                # Extract fields
                job_title = item.get("job_title") or item.get("title") or item.get("position")
                company_name = item.get("company_name") or item.get("company") or item.get("employer")
                job_url = item.get("url") or item.get("job_url") or item.get("link")
                location = item.get("location") or item.get("company_location")
                raw = item

                if not job_url:
                    logger.warning("Skipping item with no job_url: %s", item)
                    errors += 1
                    continue

                # FIX 1: Enhanced duplicate check - check both job_url AND title+company
                exists = db.query(Result).filter(Result.job_url == job_url).first()
                if exists:
                    logger.info("Duplicate job_url, skipping: %s", job_url)
                    continue
                
                # FIX 2: Additional duplicate check by title + company
                if job_title and company_name:
                    exists_by_content = db.query(Result).filter(
                        Result.job_title == job_title,
                        Result.company_name == company_name
                    ).first()
                    if exists_by_content:
                        logger.info("Duplicate job (title+company), skipping: %s at %s", job_title, company_name)
                        continue

                # domain via Clearbit
                domain = clearbit_get_domain(company_name) if company_name else None
                email = None
                if domain:
                    emails = hunter_get_emails_for_domain(domain)
                    email = emails[0] if emails else None

                # persist
                r = Result(
                    job_title=job_title,
                    company_name=company_name,
                    company_domain=domain,
                    company_location=location,
                    job_url=job_url,
                    description=item.get("description") or item.get("summary"),
                    email=email,
                    source=item.get("source") or "apify",
                    raw=raw
                )
                db.add(r)
                db.commit()
                new_rows += 1
            except Exception as e:
                logger.exception("Error processing item: %s", e)
                db.rollback()
                errors += 1
                continue

        # update run
        run.total = total
        run.new_rows = new_rows
        run.errors = errors
        run.status = "success"
        run.finished_at = func.now()
        run.meta = {"apify": {"items_fetched": total}}
        db.add(run)
        db.commit()
        logger.info("Background job finished run_id=%s total=%d new=%d errors=%d", run_id, total, new_rows, errors)
    except Exception:
        logger.exception("Unexpected error in process_run")
    finally:
        db.close()

# -------------------------
# HTTP endpoints
# -------------------------
@app.route("/", methods=["GET"])
def frontend():
    # Serve your uploaded frontend
    try:
        if not os.path.isfile(FRONTEND_PATH):
            logger.error("Frontend file not found at %s", FRONTEND_PATH)
            return "Frontend not found on server. Set FRONTEND_PATH to correct file.", 500
        return send_file(FRONTEND_PATH, mimetype="text/html")
    except Exception as e:
        logger.exception("Error serving frontend: %s", e)
        abort(500)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "version": "1.0"})

@app.route("/webhook/run-scraper", methods=["POST"])
def run_scraper():
    payload = request.get_json(silent=True) or {}
    initiated_by = payload.get("initiatedBy", "frontend")
    note = payload.get("note", "")
    
    # Add this - proper Apify input
    actor_input = {
        "keywords": "medical billing, mental health billing, behavioral health billing, psychiatry billing",
        "location": "United States", 
        "maxJobs": 1,  # Change to 1 for testing, then 30 later
        "days": 3
    }
    
    db = SessionLocal()
    try:
        new_run = Run(status="running", initiated_by=initiated_by, note=note, meta={"actor_input": actor_input})
        db.add(new_run)
        db.commit()
        run_id = new_run.id
        
        # Pass actor_input to process_run
        executor.submit(process_run, run_id, actor_input)
        
        return jsonify({"ok": True, "run_id": run_id}), 202
    except Exception as e:
        logger.exception("Failed creating run")
        db.rollback()
        return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        db.close()

@app.route("/webhook/get-results", methods=["GET"])
def get_results():
    # pagination
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    db = SessionLocal()
    try:
        q = db.query(Result).order_by(Result.scraped_at.desc()).limit(limit).offset(offset)
        items = []
        for r in q:
            items.append({
                "id": r.id,
                "job_title": r.job_title,
                "company_name": r.company_name,
                "company_domain": r.company_domain,
                "job_url": r.job_url,
                "email": r.email,
                "source": r.source,
                "scraped_at": r.scraped_at.isoformat() if r.scraped_at else None
            })
        return jsonify({"ok": True, "results": items})
    finally:
        db.close()

@app.route("/runs", methods=["GET"])
def list_runs():
    limit = int(request.args.get("limit", 20))
    db = SessionLocal()
    try:
        q = db.query(Run).order_by(Run.created_at.desc()).limit(limit)
        out = []
        for r in q:
            out.append({
                "id": r.id, "status": r.status, "initiated_by": r.initiated_by,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "total": r.total, "new_rows": r.new_rows, "errors": r.errors, "meta": r.meta
            })
        return jsonify({"ok": True, "runs": out})
    finally:
        db.close()

# -------------------------
# CLI run
# -------------------------
if __name__ == "__main__":
    logger.info("Starting Flask app on port %s", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=os.getenv("FLASK_DEBUG", "0") == "1")
