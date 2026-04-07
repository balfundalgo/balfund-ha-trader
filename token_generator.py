"""
Dhan Token Generator — GUI
Balfund Trading Private Limited

Uses the same token generation logic as dhan_token_manager.py:
  Method 1 (Generate): POST https://auth.dhan.co/app/generateAccessToken
                        ?dhanClientId={id}&pin={pin}&totp={otp}
  Method 2 (Renew):    GET  https://api.dhan.co/v2/RenewToken
                        Headers: access-token, dhanClientId

Broadcasts token via:
  - dhan_token.json  (same folder as EXE)
  - http://localhost:5555/token
"""

import customtkinter as ctk
import pyotp
import requests
import json
import threading
import os
import sys
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(sys.argv[0]))
CREDS_FILE = os.path.join(BASE_DIR, "saved_creds.json")
TOKEN_SERVER_PORT = 5555

# Shared folder — auto-created on first run on any Windows PC
SHARED_DIR = r"C:\balfund_shared"
os.makedirs(SHARED_DIR, exist_ok=True)
TOKEN_FILE = os.path.join(SHARED_DIR, "dhan_token.json")

# ── Shared token state (served over HTTP) ────────────────────────────────────
current_token_data: dict = {}


# ── Localhost HTTP Server ─────────────────────────────────────────────────────
class TokenHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/token":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(current_token_data).encode())
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error": "Use /token"}')

    def log_message(self, format, *args):
        pass  # suppress console spam


def _start_server():
    try:
        HTTPServer(("localhost", TOKEN_SERVER_PORT), TokenHandler).serve_forever()
    except OSError:
        pass  # port already in use by another instance


threading.Thread(target=_start_server, daemon=True).start()


# ── Token API Functions (mirrors dhan_token_manager.py logic) ─────────────────

def api_generate_token(client_id: str, pin: str, totp_secret: str) -> dict:
    """
    Method 1: Generate fresh token via PIN + TOTP.
    POST https://auth.dhan.co/app/generateAccessToken
         ?dhanClientId={client_id}&pin={pin}&totp={totp_code}
    """
    totp_code = pyotp.TOTP(totp_secret).now()
    url = "https://auth.dhan.co/app/generateAccessToken"
    params = {"dhanClientId": client_id, "pin": pin, "totp": totp_code}

    resp = requests.post(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    if "accessToken" in data:
        return {
            "success":      True,
            "access_token": data["accessToken"],
            "expiry":       data.get("expiryTime", ""),
            "client_name":  data.get("dhanClientName", ""),
            "method":       "GENERATED",
        }
    err = data.get("errorMessage") or data.get("message") or str(data)
    return {"success": False, "error": err}


def api_renew_token(client_id: str, access_token: str) -> dict:
    """
    Method 2: Renew an existing valid token for another 24 hours.
    GET https://api.dhan.co/v2/RenewToken
    Headers: access-token, dhanClientId
    """
    url = "https://api.dhan.co/v2/RenewToken"
    headers = {
        "access-token": access_token,
        "dhanClientId": client_id,
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    if "accessToken" in data:
        return {
            "success":      True,
            "access_token": data["accessToken"],
            "expiry":       data.get("expiryTime", ""),
            "client_name":  data.get("dhanClientName", ""),
            "method":       "RENEWED",
        }
    err = data.get("errorMessage") or data.get("message") or str(data)
    return {"success": False, "error": err}


def api_verify_token(client_id: str, access_token: str) -> bool:
    """Ping profile endpoint to check if token is still valid."""
    if not access_token:
        return False
    try:
        resp = requests.get(
            "https://api.dhan.co/v2/profile",
            headers={"access-token": access_token, "client-id": client_id},
            timeout=10
        )
        return resp.status_code == 200
    except Exception:
        return False


# ── Main GUI ──────────────────────────────────────────────────────────────────
class DhanTokenApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.title("Balfund — Dhan Token Generator")
        self.geometry("540x780")
        self.resizable(False, False)

        self._token_value = ""
        self._client_id   = ""
        self._build_ui()
        self._load_saved_creds()

    # ── UI Build ──────────────────────────────────────────────────────────────
    def _build_ui(self):

        # ── Header ────────────────────────────────────────────────────────────
        ctk.CTkLabel(
            self, text="Dhan Token Generator",
            font=ctk.CTkFont(size=22, weight="bold")
        ).pack(pady=(26, 2))

        ctk.CTkLabel(
            self, text="Balfund Trading Private Limited",
            font=ctk.CTkFont(size=12), text_color="#888888"
        ).pack(pady=(0, 18))

        # ── Credentials ───────────────────────────────────────────────────────
        cred = ctk.CTkFrame(self, corner_radius=12)
        cred.pack(fill="x", padx=26, pady=(0, 10))

        ctk.CTkLabel(
            cred, text="CREDENTIALS",
            font=ctk.CTkFont(size=10, weight="bold"), text_color="#555555"
        ).pack(anchor="w", padx=20, pady=(14, 2))

        ctk.CTkLabel(cred, text="Client ID", font=ctk.CTkFont(size=13)).pack(anchor="w", padx=20)
        self.ent_client_id = ctk.CTkEntry(
            cred, placeholder_text="Your Dhan Client ID",
            height=38, corner_radius=8
        )
        self.ent_client_id.pack(fill="x", padx=20, pady=(4, 10))

        ctk.CTkLabel(cred, text="6-Digit PIN", font=ctk.CTkFont(size=13)).pack(anchor="w", padx=20)
        self.ent_pin = ctk.CTkEntry(
            cred, placeholder_text="Your Dhan 6-digit trading PIN",
            show="*", height=38, corner_radius=8
        )
        self.ent_pin.pack(fill="x", padx=20, pady=(4, 10))

        ctk.CTkLabel(cred, text="TOTP Secret Key", font=ctk.CTkFont(size=13)).pack(anchor="w", padx=20)
        self.ent_totp = ctk.CTkEntry(
            cred, placeholder_text="Base32 secret from authenticator setup",
            show="*", height=38, corner_radius=8
        )
        self.ent_totp.pack(fill="x", padx=20, pady=(4, 10))

        self.chk_save = ctk.CTkCheckBox(
            cred, text="Save credentials locally",
            font=ctk.CTkFont(size=12)
        )
        self.chk_save.pack(anchor="w", padx=20, pady=(0, 14))

        # ── Buttons row ───────────────────────────────────────────────────────
        btn_row = ctk.CTkFrame(self, fg_color="transparent")
        btn_row.pack(fill="x", padx=26, pady=(0, 6))

        self.btn_generate = ctk.CTkButton(
            btn_row, text="Generate Token",
            height=46, corner_radius=10,
            font=ctk.CTkFont(size=14, weight="bold"),
            command=self._on_generate
        )
        self.btn_generate.pack(side="left", fill="x", expand=True, padx=(0, 6))

        self.btn_renew = ctk.CTkButton(
            btn_row, text="Renew Token",
            height=46, corner_radius=10,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color="#1a5276", hover_color="#1f618d",
            state="disabled",
            command=self._on_renew
        )
        self.btn_renew.pack(side="left", fill="x", expand=True, padx=(6, 0))

        # ── Status ────────────────────────────────────────────────────────────
        self.lbl_status = ctk.CTkLabel(
            self, text="Enter credentials and click Generate",
            font=ctk.CTkFont(size=12), text_color="#888888",
            wraplength=480
        )
        self.lbl_status.pack(pady=(4, 10))

        # ── Token Display ─────────────────────────────────────────────────────
        tok_frame = ctk.CTkFrame(self, corner_radius=12)
        tok_frame.pack(fill="x", padx=26, pady=(0, 10))

        ctk.CTkLabel(
            tok_frame, text="ACCESS TOKEN",
            font=ctk.CTkFont(size=10, weight="bold"), text_color="#555555"
        ).pack(anchor="w", padx=20, pady=(14, 4))

        # Token textbox + Copy button side by side
        token_row = ctk.CTkFrame(tok_frame, fg_color="transparent")
        token_row.pack(fill="x", padx=20, pady=(0, 6))

        self.txt_token = ctk.CTkTextbox(
            token_row, height=65, corner_radius=8,
            font=ctk.CTkFont(size=11), wrap="word"
        )
        self.txt_token.pack(side="left", fill="both", expand=True, padx=(0, 8))
        self.txt_token.configure(state="disabled")

        self.btn_copy = ctk.CTkButton(
            token_row, text="Copy",
            width=70, height=65,
            corner_radius=8,
            fg_color="#1e3a1e", hover_color="#27521f",
            border_width=1, border_color="#2d6a2d",
            font=ctk.CTkFont(size=13, weight="bold"),
            state="disabled",
            command=self._copy_token
        )
        self.btn_copy.pack(side="left")

        self.lbl_copied = ctk.CTkLabel(
            tok_frame, text="",
            font=ctk.CTkFont(size=11), text_color="#4CAF50"
        )
        self.lbl_copied.pack(anchor="e", padx=20)

        # Client info row
        info_row = ctk.CTkFrame(tok_frame, fg_color="transparent")
        info_row.pack(fill="x", padx=20, pady=(0, 14))

        self.lbl_client_name = ctk.CTkLabel(
            info_row, text="",
            font=ctk.CTkFont(size=11), text_color="#888888"
        )
        self.lbl_client_name.pack(side="left")

        self.lbl_expiry = ctk.CTkLabel(
            info_row, text="",
            font=ctk.CTkFont(size=11), text_color="#888888"
        )
        self.lbl_expiry.pack(side="right")

        # ── Broadcast Info ────────────────────────────────────────────────────
        bcast = ctk.CTkFrame(self, corner_radius=12)
        bcast.pack(fill="x", padx=26, pady=(0, 20))

        ctk.CTkLabel(
            bcast, text="BROADCAST CHANNELS",
            font=ctk.CTkFont(size=10, weight="bold"), text_color="#555555"
        ).pack(anchor="w", padx=20, pady=(14, 6))

        self.lbl_server = ctk.CTkLabel(
            bcast,
            text=f"HTTP   ->   http://localhost:{TOKEN_SERVER_PORT}/token",
            font=ctk.CTkFont(size=12), text_color="#888888"
        )
        self.lbl_server.pack(anchor="w", padx=20, pady=2)

        self.lbl_file = ctk.CTkLabel(
            bcast,
            text="File     ->   C:\\balfund_shared\\dhan_token.json  (auto-created)",
            font=ctk.CTkFont(size=11), text_color="#888888"
        )
        self.lbl_file.pack(anchor="w", padx=20, pady=(2, 14))

    # ── Button Handlers ───────────────────────────────────────────────────────
    def _on_generate(self):
        client_id   = self.ent_client_id.get().strip()
        pin         = self.ent_pin.get().strip()
        totp_secret = self.ent_totp.get().strip()

        if not all([client_id, pin, totp_secret]):
            self._set_status("Please fill in Client ID, PIN, and TOTP Secret", "#FF5252")
            return

        self._client_id = client_id
        if self.chk_save.get():
            self._save_creds(client_id, pin, totp_secret)

        self._set_buttons_loading("Generating...")
        self._set_status("Connecting to Dhan...", "#FFA726")

        threading.Thread(
            target=self._thread_generate,
            args=(client_id, pin, totp_secret),
            daemon=True
        ).start()

    def _on_renew(self):
        if not self._token_value or not self._client_id:
            self._set_status("No active token to renew", "#FF5252")
            return

        self._set_buttons_loading("Renewing...")
        self._set_status("Renewing token...", "#FFA726")

        threading.Thread(
            target=self._thread_renew,
            args=(self._client_id, self._token_value),
            daemon=True
        ).start()

    # ── Worker Threads ────────────────────────────────────────────────────────
    def _thread_generate(self, client_id, pin, totp_secret):
        try:
            result = api_generate_token(client_id, pin, totp_secret)
            if result["success"]:
                self.after(0, lambda: self._on_success(result))
            else:
                self.after(0, lambda: self._on_error(result["error"]))
        except requests.exceptions.HTTPError as e:
            self.after(0, lambda: self._on_error(f"HTTP {e.response.status_code}: {e.response.text[:120]}"))
        except requests.exceptions.Timeout:
            self.after(0, lambda: self._on_error("Request timed out. Check internet connection."))
        except Exception as e:
            self.after(0, lambda: self._on_error(str(e)))

    def _thread_renew(self, client_id, access_token):
        try:
            result = api_renew_token(client_id, access_token)
            if result["success"]:
                self.after(0, lambda: self._on_success(result))
            else:
                self.after(0, lambda: self._on_error(f"Renew failed: {result['error']}"))
        except requests.exceptions.HTTPError as e:
            self.after(0, lambda: self._on_error(f"Renew failed (token may be expired): HTTP {e.response.status_code}"))
        except Exception as e:
            self.after(0, lambda: self._on_error(str(e)))

    # ── Success / Error ───────────────────────────────────────────────────────
    def _on_success(self, result: dict):
        global current_token_data

        token       = result["access_token"]
        expiry      = result.get("expiry", "")
        client_name = result.get("client_name", "")
        method      = result.get("method", "")
        ts          = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self._token_value = token
        self._client_id   = self._client_id or self.ent_client_id.get().strip()

        # Broadcast
        current_token_data = {
            "client_id":    self._client_id,
            "access_token": token,
            "generated_at": ts,
            "expiry":       expiry,
            "client_name":  client_name,
        }

        file_ok = False
        try:
            with open(TOKEN_FILE, "w") as f:
                json.dump(current_token_data, f, indent=2)
            file_ok = True
        except Exception:
            pass

        # Update token display
        self.txt_token.configure(state="normal")
        self.txt_token.delete("1.0", "end")
        self.txt_token.insert("1.0", token)
        self.txt_token.configure(state="disabled")

        # Update labels
        self.lbl_client_name.configure(
            text=f"  {client_name}" if client_name else "",
            text_color="#4CAF50"
        )
        if expiry:
            try:
                exp_dt = datetime.fromisoformat(expiry.replace(".000", ""))
                self.lbl_expiry.configure(
                    text=f"Expires: {exp_dt.strftime('%d %b %Y  %H:%M')}  ",
                    text_color="#888888"
                )
            except Exception:
                self.lbl_expiry.configure(text=f"Expires: {expiry}  ", text_color="#888888")

        # Enable buttons
        self.btn_copy.configure(state="normal")
        self.btn_renew.configure(state="normal")
        self._set_buttons_ready()

        # Broadcast channel indicators
        self.lbl_server.configure(text_color="#4CAF50")
        if file_ok:
            self.lbl_file.configure(text_color="#4CAF50")

        verb = "generated" if method == "GENERATED" else "renewed"
        file_note = "" if file_ok else "  (file write failed)"
        self._set_status(f"Token {verb} successfully{file_note}  |  {ts}", "#4CAF50")

    def _on_error(self, message: str):
        self._set_status(f"Error: {message}", "#FF5252")
        self._set_buttons_ready()

    # ── Copy ──────────────────────────────────────────────────────────────────
    def _copy_token(self):
        if self._token_value:
            self.clipboard_clear()
            self.clipboard_append(self._token_value)
            self.btn_copy.configure(text="Done!")
            self.after(2000, lambda: self.btn_copy.configure(text="Copy"))

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _set_status(self, msg, color="#888888"):
        self.lbl_status.configure(text=msg, text_color=color)

    def _set_buttons_loading(self, label: str):
        self.btn_generate.configure(state="disabled", text=label)
        self.btn_renew.configure(state="disabled")

    def _set_buttons_ready(self):
        self.btn_generate.configure(state="normal", text="Generate Token")
        if self._token_value:
            self.btn_renew.configure(state="normal")

    # ── Credential Persistence ────────────────────────────────────────────────
    def _save_creds(self, client_id, pin, totp_secret):
        try:
            with open(CREDS_FILE, "w") as f:
                json.dump({"client_id": client_id, "pin": pin, "totp_secret": totp_secret}, f)
        except Exception:
            pass

    def _load_saved_creds(self):
        if not os.path.exists(CREDS_FILE):
            return
        try:
            with open(CREDS_FILE) as f:
                data = json.load(f)
            self.ent_client_id.insert(0, data.get("client_id", ""))
            self.ent_pin.insert(0, data.get("pin", ""))
            self.ent_totp.insert(0, data.get("totp_secret", ""))
            self.chk_save.select()
        except Exception:
            pass


if __name__ == "__main__":
    app = DhanTokenApp()
    app.mainloop()
