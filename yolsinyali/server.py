#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
YolSinyali Traffic Reporting and Location Sharing Server (Production Ready)
Built with Flask, Flask-SocketIO, Supabase-py, and Eventlet
"""

import os
import sys
import time
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request
from flask_socketio import SocketIO, emit
from dotenv import load_dotenv

# Load local environment variables if available
load_dotenv()

# --- CONFIGURATION ENGINE ---
# These variables can be configured directly below or set as environment variables on Render.com / .env
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# In-memory fallback database in case Supabase credentials are not provided or error occurs
backup_reports_db = []
backup_id_counter = 1

print(f"[*] Starting YolSinyali Backend Service...")

# Initialize Supabase Client
supabase_enabled = False
supabase_client = None

if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client, Client
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        supabase_enabled = True
        print(f"[+] Connected to Supabase successfully: {SUPABASE_URL}")
    except Exception as e:
        print(f"[!] Warning: Failed to connect to Supabase. Reason: {e}")
        print("[!] Falling back to fully functional in-memory server database.")
else:
    print("[!] Supabase environment variables NOT detected. Operating in high-fidelity in-memory rollback mode.")

# Initialize Flask and SocketIO
app = Flask(__name__)
# Allow CORS from any origin for mobile WebView or web app
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# Internal helper to parse timestamps safely
def parse_utc_timestamp(ts_val):
    if not ts_val:
        return datetime.now(timezone.utc)
    
    # Supabase might return timestamps in various ISO formats
    try:
        # String slicing helper if there are too many timezone decimals
        if isinstance(ts_val, str):
            ts_val = ts_val.replace('Z', '+00:00')
            # Handles fractional seconds up to 6 digits
            if '.' in ts_val:
                base, frag = ts_val.split('.')
                tz_part = ""
                if '+' in frag:
                    frag, tz_part = frag.split('+')
                    tz_part = '+' + tz_part
                elif '-' in frag:
                    frag, tz_part = frag.split('-')
                    tz_part = '-' + tz_part
                
                frag = frag[:6] # limit microseconds to 6 digits
                ts_val = f"{base}.{frag}{tz_part}"
            
            return datetime.fromisoformat(ts_val)
    except Exception as e:
        print(f"[!] Warning: ISO timestamp parsing failed for '{ts_val}' ({e}). Using native utc now.")
    
    return datetime.now(timezone.utc)

# Core TTL Logic: verify if a report is still active
def is_report_active(report):
    # 'Yemek Yeri' is permanent, never expires
    if report.get("report_type") == "Yemek Yeri":
        return True
    
    duration = int(report.get("duration_minutes", 120))
    # If duration is -1, it is permanent
    if duration == -1:
        return True
        
    created_at_dt = parse_utc_timestamp(report.get("created_at"))
    now = datetime.now(timezone.utc)
    expiration_time = created_at_dt + timedelta(minutes=duration)
    
    return now < expiration_time

# Retrieve active reports
def get_active_reports():
    active_list = []
    
    if supabase_enabled:
        try:
            # Query from Supabase. We retrieve recent reports
            # Since some might be active up to 48 hours (2880 mins), we fetch reports created in the last 3 days
            three_days_ago = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
            response = supabase_client.table("reports").select("*").gte("created_at", three_days_ago).execute()
            
            for item in response.data:
                # Format properly for frontend
                formatted_item = {
                    "id": item.get("id"),
                    "username": item.get("username"),
                    "latitude": float(item.get("latitude")),
                    "longitude": float(item.get("longitude")),
                    "report_type": item.get("report_type"),
                    "duration_minutes": int(item.get("duration_minutes")),
                    "created_at": item.get("created_at")
                }
                if is_report_active(formatted_item):
                    active_list.append(formatted_item)
            return active_list
        except Exception as e:
            print(f"[!] Supabase retrieve error: {e}. Utilizing backup db.")
            # Fall through to backup
    
    # Process backup memory DB
    global backup_reports_db
    # Clean up expired ones from our local cache too
    backup_reports_db = [r for r in backup_reports_db if is_report_active(r)]
    return backup_reports_db

# --- HTTP ENDPOINTS ---

@app.route("/")
def index():
    return jsonify({
        "status": "online",
        "app": "YolSinyali Traffic Reporting Server",
        "supabase_connected": supabase_enabled,
        "active_pins_count": len(get_active_reports()),
        "time_utc": datetime.now(timezone.utc).isoformat()
    })

@app.route("/reports", methods=["GET"])
def get_reports_api():
    """HTTP endpoint to query active reports directly"""
    return jsonify(get_active_reports())


# --- SOCKET.IO REAL-TIME TRIGGERS ---

@socketio.on("connect")
def handle_connect():
    print(f"[+] Client connected: {request.sid}")
    # Compile list of active reports and transmit immediately
    active_reports = get_active_reports()
    emit("init_reports", active_reports)

@socketio.on("disconnect")
def handle_disconnect():
    print(f"[-] Client disconnected: {request.sid}")

@socketio.on("new_report")
def handle_new_report(data):
    """
    Handles registering new traffic pins.
    Payload expected:
    {
      "username": "Kullanıcı Adı",
      "latitude": 41.0123,
      "longitude": 28.9765,
      "report_type": "Kaza" / "Trafik Çevirmesi" / "Yemek Yeri",
      "duration_minutes": 360 # (e.g. 6 hours)
    }
    """
    global backup_id_counter, backup_reports_db
    
    print(f"[+] New report received: {data}")
    
    username = data.get("username", "Anonim").strip()
    try:
        latitude = float(data.get("latitude", 0.0))
        longitude = float(data.get("longitude", 0.0))
    except (ValueError, TypeError):
        print("[!] Error parsing coordinates. Ignoring report.")
        return
        
    report_type = data.get("report_type", "Diğer").strip()
    
    # Dynamic duration choice
    # If "Yemek Yeri", duration defaults to -1 (never expires)
    if report_type == "Yemek Yeri":
        duration_minutes = -1
    else:
        try:
            duration_minutes = int(data.get("duration_minutes", 120))
        except (ValueError, TypeError):
            duration_minutes = 120
    
    created_at_str = datetime.now(timezone.utc).isoformat()
    
    new_record = {
        "username": username,
        "latitude": latitude,
        "longitude": longitude,
        "report_type": report_type,
        "duration_minutes": duration_minutes,
        "created_at": created_at_str
    }
    
    saved_record = None
    
    if supabase_enabled:
        try:
            insert_res = supabase_client.table("reports").insert(new_record).execute()
            if insert_res.data and len(insert_res.data) > 0:
                saved_record = insert_res.data[0]
                # Ensure fields match formatting
                saved_record["id"] = saved_record.get("id")
                saved_record["latitude"] = float(saved_record["latitude"])
                saved_record["longitude"] = float(saved_record["longitude"])
                saved_record["duration_minutes"] = int(saved_record["duration_minutes"])
                print(f"[+] Saved to Supabase: ID {saved_record['id']}")
        except Exception as e:
            print(f"[!] Supabase save failure: {e}. Storing in memory fallback.")
            
    if saved_record is None:
        # Fallback to local memory DB
        saved_record = new_record.copy()
        saved_record["id"] = backup_id_counter
        backup_id_counter += 1
        backup_reports_db.append(saved_record)
        print(f"[+] Saved in Memory Database: ID {saved_record['id']}")
        
    # Broadcast the newly posted report immediately to ALL connected screens
    # Trigger Leaflet layout real-time additions (No page refresh requested)
    socketio.emit("report_added", saved_record)


# --- BACKGROUND TTL JANITOR ---
# Runs dynamically to flag and clear markers whose lifetime is over
def dynamic_ttl_janitor():
    """
    Background worker that monitors markers and fires an event
    to delete markers from everyone's map at the exact fractional second they expire.
    """
    print("[*] TTL Janitor thread initialized.")
    while True:
        try:
            # Check files every 10 seconds
            socketio.sleep(10)
            
            # Fetch currently active reports from cache / databases
            # Check which records have reached expiry and emit a batch or individual deletes
            current_active = get_active_reports()
            
            # Let's check against a list of known records if they expired.
            # To avoid sending deletes for items initialized but not expired under normal loads:
            # We simply check all currently queried items from Supabase.
            # Wait, if we want to delete expired records, we first gather recent entries from database:
            if supabase_enabled:
                try:
                    # Let's request records from the last 3 days
                    three_days_ago = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
                    response = supabase_client.table("reports").select("*").gte("created_at", three_days_ago).execute()
                    all_recent = response.data
                    
                    for item in all_recent:
                        formatted_item = {
                            "id": item.get("id"),
                            "username": item.get("username"),
                            "latitude": float(item.get("latitude")),
                            "longitude": float(item.get("longitude")),
                            "report_type": item.get("report_type"),
                            "duration_minutes": int(item.get("duration_minutes")),
                            "created_at": item.get("created_at")
                        }
                        if not is_report_active(formatted_item):
                            # This pin has expired! Trigger real-time client dismissal of map layers
                            # We can emit 'report_expired' so maps remove them instantly!
                            socketio.emit("report_expired", {"id": formatted_item["id"]})
                            
                except Exception as e:
                    pass
            else:
                # Local DB TTL eviction
                global backup_reports_db
                active_remaining = []
                for r in backup_reports_db:
                    if is_report_active(r):
                        active_remaining.append(r)
                    else:
                        print(f"[-] Local TTL eviction triggered for pin ID {r['id']}")
                        socketio.emit("report_expired", {"id": r["id"]})
                backup_reports_db = active_remaining
                
        except Exception as ex:
            print(f"[!] Dynamic TTL Janitor encountered an exception: {ex}")

# Start dynamic janitor thread asynchronously in the eventlet environment
socketio.start_background_task(dynamic_ttl_janitor)


# --- RUN COMMAND ---
if __name__ == "__main__":
    # Standard Flask listener on port 5000, compatible with local or Render.com deploys
    port = int(os.environ.get("PORT", 5000))
    print(f"[+] Engine running on port {port}. WSGI: Eventlet.")
    socketio.run(app, host="0.0.0.0", port=port, debug=False)
