from __future__ import annotations

import json
import struct
import threading
import time
from typing import Any, Callable, Dict, Optional

import websocket


REQ_SUB_TICKER = 15
RESP_TICKER = 2
RESP_PREV_CLOSE = 6
RESP_DISCONNECT = 50

EXCH_SEG_MAP_NUM_TO_NAME = {
    0: "IDX_I",
    1: "NSE_EQ",
    2: "NSE_FNO",
    3: "NSE_CURRENCY",
    4: "BSE_EQ",
    5: "MCX_COMM",
    7: "BSE_CURRENCY",
    8: "BSE_FNO",
}


def _normalize_dhan_epoch(ts: int) -> int:
    """
    Dhan WS sometimes sends ltt with an offset effect in some environments.
    If timestamp appears ahead by ~5.5h, normalize it.
    """
    ts = int(ts)
    now_ts = int(time.time())
    diff = ts - now_ts
    if int(4.5 * 3600) <= diff <= int(6.5 * 3600):
        ts -= 19800
    return ts


def parse_header_8(msg: bytes) -> Optional[Dict[str, Any]]:
    if len(msg) < 8:
        return None

    resp_code = msg[0]
    msg_len = struct.unpack_from("<H", msg, 1)[0]
    exch_seg_num = msg[3]
    sec_id_i = struct.unpack_from("<I", msg, 4)[0]

    return {
        "resp_code": int(resp_code),
        "msg_len": int(msg_len),
        "exch_seg_num": int(exch_seg_num),
        "exch_seg_name": EXCH_SEG_MAP_NUM_TO_NAME.get(int(exch_seg_num), str(exch_seg_num)),
        "security_id": str(sec_id_i),
        "payload": msg[8:],
    }


def parse_ticker(payload: bytes) -> Optional[Dict[str, Any]]:
    if len(payload) < 8:
        return None

    ltp = struct.unpack_from("<f", payload, 0)[0]
    ltt = struct.unpack_from("<I", payload, 4)[0]

    return {
        "ltp": float(ltp),
        "ltt_epoch": _normalize_dhan_epoch(int(ltt)),
    }


def parse_prev_close(payload: bytes) -> Optional[Dict[str, Any]]:
    if len(payload) < 8:
        return None

    prev_close = struct.unpack_from("<f", payload, 0)[0]
    prev_oi = struct.unpack_from("<I", payload, 4)[0]

    return {
        "prev_close": float(prev_close),
        "prev_oi": int(prev_oi),
    }


class DhanWSClient:
    def __init__(
        self,
        client_id: str,
        access_token: str,
        exchange_segment: str,
        security_id: str,
        on_tick: Callable[[float, int], None],
        on_status: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.client_id = str(client_id).strip()
        self.access_token = str(access_token).strip()
        self.exchange_segment = str(exchange_segment).strip()
        self.security_id = str(security_id).strip()

        self.on_tick_callback = on_tick
        self.on_status_callback = on_status

        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()

        self.last_prev_close: Optional[float] = None
        self.last_ticker_key: Optional[tuple] = None
        self.last_packet_time: Optional[float] = None
        self.last_connect_time: Optional[float] = None

        self.packet_counts: Dict[Any, int] = {
            RESP_TICKER: 0,
            RESP_PREV_CLOSE: 0,
            RESP_DISCONNECT: 0,
            "other": 0,
        }

    @property
    def ws_url(self) -> str:
        return (
            f"wss://api-feed.dhan.co?version=2"
            f"&token={self.access_token}"
            f"&clientId={self.client_id}"
            f"&authType=2"
        )

    def _status(self, msg: str) -> None:
        if self.on_status_callback:
            try:
                self.on_status_callback(msg)
            except Exception:
                pass

    def _on_open(self, ws) -> None:
        self.last_connect_time = time.time()
        self._status("Connecting websocket...")

        sub_msg = {
            "RequestCode": REQ_SUB_TICKER,
            "InstrumentCount": 1,
            "InstrumentList": [
                {
                    "ExchangeSegment": self.exchange_segment,
                    "SecurityId": self.security_id,
                }
            ],
        }

        ws.send(json.dumps(sub_msg))
        self._status(f"Subscribed to {self.exchange_segment}:{self.security_id}")

    def _on_message(self, ws, message) -> None:
        try:
            if isinstance(message, str):
                return

            msg = bytes(message)
            hdr = parse_header_8(msg)
            if not hdr:
                return

            code = int(hdr["resp_code"])
            sec = str(hdr["security_id"])

            if sec != self.security_id:
                return

            self.last_packet_time = time.time()

            if code == RESP_TICKER:
                t = parse_ticker(hdr["payload"])
                if not t:
                    return

                ltp = float(t["ltp"])
                ltt_epoch = int(t["ltt_epoch"])

                # prevent exact duplicate packet double counting
                k = (round(ltp, 8), ltt_epoch)
                if self.last_ticker_key == k:
                    return
                self.last_ticker_key = k

                self.packet_counts[RESP_TICKER] = self.packet_counts.get(RESP_TICKER, 0) + 1
                self.on_tick_callback(ltp, ltt_epoch)
                return

            if code == RESP_PREV_CLOSE:
                p = parse_prev_close(hdr["payload"])
                if p:
                    self.last_prev_close = float(p["prev_close"])
                    self.packet_counts[RESP_PREV_CLOSE] = self.packet_counts.get(RESP_PREV_CLOSE, 0) + 1
                return

            if code == RESP_DISCONNECT:
                self.packet_counts[RESP_DISCONNECT] = self.packet_counts.get(RESP_DISCONNECT, 0) + 1
                self._status("Feed disconnect packet received")
                return

            self.packet_counts["other"] = self.packet_counts.get("other", 0) + 1

        except Exception as e:
            self._status(f"WS parse error: {e}")

    def _on_error(self, ws, error) -> None:
        self._status(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        self._status(f"WebSocket closed: code={close_status_code}, msg={close_msg}")

    def _run_forever(self) -> None:
        websocket.enableTrace(False)

        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                self._status(f"WS exception: {e}")

            if not self.stop_event.is_set():
                time.sleep(2)

    def start(self) -> None:
        self.stop_event.clear()
        self.ws_thread = threading.Thread(target=self._run_forever, daemon=True)
        self.ws_thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass