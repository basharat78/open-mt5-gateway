"""Pre-flight diagnostic CLI & local simulator for OpenMT5Gateway (`open-mt5-preflight`)."""

from __future__ import annotations

import argparse
import json
import socket
import threading
from datetime import datetime, timezone
from typing import Any, Sequence

from open_mt5_gateway.gateway import GatewayConfig, OpenMT5Client, OpenMT5GatewayServer


def _start_demo_simulator(
    port: int,
    token: str,
    account_login: str,
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()
    quotes: dict[str, tuple[str, str, str, str, int]] = {
        "EURUSD": ("EURUSD.a", "1.08500", "1.08510", "0.00001", 5),
        "GBPUSD": ("GBPUSD.a", "1.26800", "1.26814", "0.00001", 5),
        "USDJPY": ("USDJPY.a", "154.200", "154.215", "0.001", 3),
        "XAUUSD": ("XAUUSD.a", "2650.10", "2650.35", "0.01", 2),
    }

    def _run() -> None:
        sock = socket.create_connection(("127.0.0.1", port), timeout=1.0)
        try:
            reader = sock.makefile("r", encoding="utf-8")
            if token:
                sock.sendall((json.dumps({"type": "AUTH", "token": token}) + "\n").encode("utf-8"))
                reader.readline()
            while not stop_event.is_set():
                try:
                    line = reader.readline()
                except TimeoutError:
                    continue
                if not line:
                    break
                msg = json.loads(line.strip())
                if msg.get("type") != "COMMAND":
                    continue
                req_id = msg["request_id"]
                action = msg["action"]
                payload = msg.get("payload", {})
                if action == "GET_ACCOUNT":
                    resp: dict[str, Any] = {
                        "type": "RESPONSE",
                        "request_id": req_id,
                        "ok": True,
                        "login": account_login,
                        "server": "Demo-Server (Simulated)",
                        "balance": "50000.00000",
                        "equity": "50000.00000",
                        "server_time": int(datetime.now(timezone.utc).timestamp()),
                    }
                elif action == "GET_SYMBOL_SPEC":
                    req_sym = str(payload.get("symbol", "")).upper()
                    resolved, bid, ask, pt, digits = quotes.get(
                        req_sym, (f"{req_sym}.a", "1.00000", "1.00012", "0.00001", 5)
                    )
                    resp = {
                        "type": "RESPONSE",
                        "request_id": req_id,
                        "ok": True,
                        "resolved_symbol": resolved,
                        "bid": bid,
                        "ask": ask,
                        "point": pt,
                        "digits": digits,
                        "volume_min": "0.01",
                        "volume_max": "100.00",
                        "volume_step": "0.01",
                        "trade_allowed": True,
                    }
                else:
                    resp = {"type": "RESPONSE", "request_id": req_id, "ok": True}
                sock.sendall((json.dumps(resp) + "\n").encode("utf-8"))
        finally:
            try:
                sock.close()
            except OSError:
                pass

    thread = threading.Thread(target=_run, name="open-mt5-demo-sim", daemon=True)
    thread.start()
    return stop_event, thread


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify MetaTrader 5 terminal connectivity.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18812)
    parser.add_argument("--token", default="")
    parser.add_argument("--wait-seconds", type=float, default=15.0)
    parser.add_argument("--symbols", nargs="+", default=["EURUSD", "GBPUSD", "XAUUSD"])
    parser.add_argument("--max-spread-pips", type=float, default=3.0)
    parser.add_argument("--simulate-demo", action="store_true")
    parser.add_argument("--account-login", default="DEMO-50K")
    args = parser.parse_args(argv)

    with OpenMT5GatewayServer(
        GatewayConfig(host=args.host, port=args.port, auth_token=args.token)
    ) as gw:
        sim_stop: threading.Event | None = None
        sim_thread: threading.Thread | None = None
        try:
            if args.simulate_demo:
                sim_stop, sim_thread = _start_demo_simulator(
                    gw.bound_port,
                    args.token,
                    args.account_login,
                )
            connected = gw.wait_for_connection(timeout=args.wait_seconds)
            if not connected:
                print(
                    json.dumps(
                        {
                            "passed": False,
                            "connected": False,
                            "error": "No OpenMT5Gateway MQL5 EA client connected.",
                        },
                        indent=2,
                    )
                )
                return 1

            client = OpenMT5Client(gw)
            acct = client.get_account()
            skew = abs((acct.server_time_utc - datetime.now(timezone.utc)).total_seconds())
            sym_results = []
            passed = True
            for sym in args.symbols:
                try:
                    spec = client.get_symbol_spec(sym)
                    spread = spec.spread_pips
                    ok = spec.trade_allowed and spread <= args.max_spread_pips
                    if not ok:
                        passed = False
                    sym_results.append(
                        {
                            "requested": sym,
                            "resolved": spec.resolved_symbol,
                            "ok": ok,
                            "spread_pips": spread,
                        }
                    )
                except RuntimeError as exc:
                    passed = False
                    sym_results.append(
                        {"requested": sym, "resolved": None, "ok": False, "error": str(exc)}
                    )

            print(
                json.dumps(
                    {
                        "passed": passed,
                        "connected": True,
                        "login": acct.login,
                        "server": acct.server,
                        "balance": str(acct.balance),
                        "equity": str(acct.equity),
                        "clock_skew_seconds": skew,
                        "symbols": sym_results,
                    },
                    indent=2,
                )
            )
            return 0 if passed else 1
        finally:
            if sim_stop is not None:
                sim_stop.set()
            if sim_thread is not None:
                sim_thread.join(timeout=1.0)


if __name__ == "__main__":
    raise SystemExit(main())
