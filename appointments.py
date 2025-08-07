#!/usr/bin/env python3
"""
st_appointments_to_drive.py
───────────────────────────
• Reads job_data.csv from Drive (by file name + folder ID)
• Extracts first/last appointment IDs
• Downloads missing appointments from ServiceTitan (batch 50)
• Appends each batch to appointments_dump.csv *directly on Drive*
  using the user OAuth helpers you provided.
"""

from __future__ import annotations
import io, os, pickle, time, logging, sys
from pathlib import Path

import pandas as pd
import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

# ─── A. GOOGLE-DRIVE OAUTH HELPERS (YOUR SNIPPET) ─────────────────────
CLIENT_SECRET_FILE = r"client_secret_740594783744-bgsb4ditlgb5u4b7d63sosj8ku7l50ba.apps.googleusercontent.com.json"
TOKEN_PICKLE       = r"token.pkl"
SCOPES             = ["https://www.googleapis.com/auth/drive.file"]  # read/write

def drive_service():
    creds = None
    if os.path.exists(TOKEN_PICKLE):
        creds = pickle.load(open(TOKEN_PICKLE, "rb"))
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        pickle.dump(creds, open(TOKEN_PICKLE, "wb"))
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def find_file_id(svc, name: str, folder_id: str) -> str | None:
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    res = svc.files().list(q=q, spaces="drive", fields="files(id)", pageSize=1).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def read_drive_csv(svc, fid: str) -> pd.DataFrame:
    buf = io.BytesIO()
    MediaIoBaseDownload(buf, svc.files().get_media(fileId=fid)).next_chunk()
    buf.seek(0)
    try:
        return pd.read_csv(buf, low_memory=False)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

def append_drive_csv(svc, df: pd.DataFrame, fid: str):
    prior = read_drive_csv(svc, fid)
    updated = pd.concat([prior, df], ignore_index=True)
    buf = io.BytesIO(); updated.to_csv(buf, index=False); buf.seek(0)
    svc.files().update(
        fileId=fid,
        media_body=MediaIoBaseUpload(buf, mimetype="text/csv", resumable=True)
    ).execute()

# ─── B. DRIVE CONFIG (FOLDER & FILENAMES) ─────────────────────────────
FOLDER_ID        = "1t-vJ8IV2b4ebHA1T2SCoPLUMT26JgzjM"
JOB_FILE_NAME    = "job_data.csv"
DUMP_FILE_NAME   = "appointments_dump.csv"

# ─── C. SERVICETITAN CONFIG ───────────────────────────────────────────
CLIENT_ID     = "cid.lz91bsv6oyhzq29ceb0r9g80z"
CLIENT_SECRET = "cs1.dzmosw0zu9jlhl5e0ymqkqpd04adtbc0y1am5tpugzfglcom47"
APP_KEY       = "ak1.nb1udeer5otcqp6yz34f50dq9"
TENANT_ID     = "875946535"
TOKEN_URL     = "https://auth.servicetitan.io/connect/token"

BATCH_SIZE    = 50

# ─── D. SERVICETITAN HELPERS ──────────────────────────────────────────
def st_token() -> str:
    r = requests.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials",
              "client_id": CLIENT_ID,
              "client_secret": CLIENT_SECRET},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def fetch_appt(aid: int, token: str) -> dict:
    url = f"https://api.servicetitan.io/jpm/v2/tenant/{TENANT_ID}/appointments/{aid}"
    hdr = {"Authorization": f"Bearer {token}",
           "ST-App-Key": APP_KEY,
           "Accept": "application/json"}
    r = requests.get(url, headers=hdr, timeout=30)
    if r.status_code == 404:
        return {"id": aid, "status": "NOT_FOUND"}
    r.raise_for_status()
    data = r.json(); data.setdefault("id", aid); return data

# ─── E. MAIN WORKFLOW ─────────────────────────────────────────────────
def main():
    svc = drive_service()

    # 1. locate files in Drive
    job_id  = find_file_id(svc, JOB_FILE_NAME,  FOLDER_ID)
    dump_id = find_file_id(svc, DUMP_FILE_NAME, FOLDER_ID)

    if not job_id:
        sys.exit(f"❌ {JOB_FILE_NAME} not found in folder.")
    if not dump_id:
        # create an empty dump file first time
        buf = io.BytesIO(b"id\n"); buf.seek(0)
        meta = {"name": DUMP_FILE_NAME, "parents": [FOLDER_ID], "mimeType": "text/csv"}
        dump_id = svc.files().create(body=meta,
                                     media_body=MediaIoBaseUpload(buf, mimetype="text/csv")).execute()["id"]
        print(f"🆕  Created {DUMP_FILE_NAME} in Drive.")

    # 2. read job_data & existing dump
    job_df  = read_drive_csv(svc, job_id)
    dump_df = read_drive_csv(svc, dump_id)
    done_ids = set(dump_df["id"].astype(int)) if not dump_df.empty else set()

    id_cols = [c for c in job_df.columns if c.lower() in {"firstappointmentid", "lastappointmentid"}]
    if not id_cols:
        sys.exit("❌ job_data.csv lacks firstappointmentid / lastappointmentid columns.")

    todo_ids = (
        pd.concat([job_df[c] for c in id_cols])
          .dropna().astype(int).unique().tolist()
    )
    todo_ids = [i for i in todo_ids if i not in done_ids]
    if not todo_ids:
        print("✔ appointments_dump.csv already up-to-date.")
        return

    print(f"🔎  {len(todo_ids):,} new appointments to fetch …")
    token  = st_token()
    batch  = []
    count  = 0

    for aid in todo_ids:
        try:
            batch.append(fetch_appt(aid, token))
        except Exception as e:
            logging.warning("ID %s: %s", aid, e)
            continue
        count += 1
        if count % BATCH_SIZE == 0:
            append_drive_csv(svc, pd.DataFrame(batch), dump_id)
            batch.clear()
            token = st_token()
            print(f"💾  {count:,} downloaded – Drive file updated.")

    # final flush
    append_drive_csv(svc, pd.DataFrame(batch), dump_id)
    print(f"✅  Finished – {count:,} new rows appended to {DUMP_FILE_NAME}")

if __name__ == "__main__":
    main()
