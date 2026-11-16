"""Tests for standalone `open_mt5_gateway` package."""

from __future__ import annotations

import json
import socket
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from open_mt5_gateway import (
    GatewayConfig,
    OpenMT5Client,
    OpenMT5GatewayServer,
    OrderRequest,
)
from open_mt5_gateway.cli import main as cli_main


class _MockEAClient:
    def __init__(self, port: int, token: str) -> None:
        self.port = port
        self.token = token
        self.sock: socket.socket | None = None
        self.auth_ack: dict[str, object] | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self.sock = socket.create_connection(("127.0.0.1", self.port), timeout=2.0)
        self._send({"type": "AUTH", "token": self.token})
        reader = self.sock.makefile("r", encoding="utf-8")
        ack = reader.readline().strip()
        if ack:
            self.auth_ack = json.loads(ack)

        def _loop() -> None:
            now_ts = int(datetime.now(timezone.utc).timestamp())
            while not self._stop.is_set():
                try:
                    line = reader.readline()
                except TimeoutError:
                    continue
                except OSError:
                    break
                if not line:
                    break
                msg = json.loads(line.strip())
                if msg.get("type") != "COMMAND":
                    continue
                req_id = msg["request_id"]
                action = msg["action"]
                if action == "PING":
                    self._send({"type": "RESPONSE", "request_id": req_id, "ok": True, "pong": True})
                elif action == "GET_ACCOUNT":
                    self._send(
                        {
                            "type": "RESPONSE",
                            "request_id": req_id,
                            "ok": True,
                            "login": "50123456",
                            "server": "FOREX.comGlobal-Demo",
                            "balance": "50000.00000",
                            "equity": "50120.00000",
                            "server_time": now_ts,
                        }
                    )
                elif action == "GET_SYMBOL_SPEC":
                    self._send(
                        {
                            "type": "RESPONSE",
                            "request_id": req_id,
                            "ok": True,
                            "resolved_symbol": "EURUSD.a",
                            "bid": "1.08500",
                            "ask": "1.08510",
                            "point": "0.00001",
                            "digits": 5,
                            "volume_min": "0.01",
                            "volume_max": "100.00",
                            "volume_step": "0.01",
                            "trade_allowed": True,
                        }
                    )
                elif action == "GET_RATES":
                    self._send(
                        {
                            "type": "RESPONSE",
                            "request_id": req_id,
                            "ok": True,
                            "rates": [
                                {
                                    "time": now_ts - 900,
                                    "open": 1.084,
                                    "high": 1.085,
                                    "low": 1.083,
                                    "close": 1.0848,
                                    "volume": 250,
                                }
                            ],
                        }
                    )
                elif action == "ORDER_SEND":
                    self._send(
                        {
                            "type": "RESPONSE",
                            "request_id": req_id,
                            "ok": True,
                            "ticket": "900123",
                            "filled_price": "1.08510",
                        }
                    )
                elif action in {"ORDER_MODIFY", "POSITION_CLOSE_PARTIAL"}:
                    self._send({"type": "RESPONSE", "request_id": req_id, "ok": True})
                elif action == "CLOSE_ALL":
                    self._send({"type": "RESPONSE", "request_id": req_id, "ok": True, "closed_count": 1})
                elif action == "GET_CLOSED_DEALS":
                    self._send(
                        {
                            "type": "RESPONSE",
                            "request_id": req_id,
                            "ok": True,
                            "deals": [
                                {
                                    "ticket": "900123",
                                    "symbol": "EURUSD.a",
                                    "side": "buy",
                                    "volume": "0.50",
                                    "open_price": "1.08510",
                                    "close_price": "1.08910",
                                    "profit": "200.00",
                                    "commission": "-3.50",
                                    "swap": "0.00",
                                    "closed_at": now_ts,
                                    "comment": "ord-1",
                                }
                            ],
                        }
                    )

        self._thread = threading.Thread(target=_loop, daemon=True)
        self._thread.start()

    def send_telemetry(self) -> None:
        self._send(
            {
                "type": "TELEMETRY",
                "balance": "50000.00000",
                "equity": "50120.00000",
                "timestamp": int(datetime.now(timezone.utc).timestamp()),
                "open_positions": 1,
            }
        )

    def _send(self, payload: dict[str, object]) -> None:
        assert self.sock is not None
        self.sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))

    def stop(self) -> None:
        self._stop.set()
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=1.0)


def test_auth_rpc_idempotency_and_telemetry() -> None:
    with OpenMT5GatewayServer(GatewayConfig(port=0, auth_token="open-secret")) as gw:
        bad = _MockEAClient(gw.bound_port, token="bad")
        bad.start()
        try:
            assert bad.auth_ack == {"type": "AUTH_ACK", "ok": False, "error": "invalid_token"}
            assert gw.is_connected is False
        finally:
            bad.stop()

        ea = _MockEAClient(gw.bound_port, token="open-secret")
        ea.start()
        try:
            assert gw.wait_for_connection(timeout=2.0) is True
            client = OpenMT5Client(gw)
            assert client.ping() is True
            acct = client.get_account()
            assert acct.login == "50123456"
            assert acct.balance == Decimal("50000.00000")

            spec = client.get_symbol_spec("EURUSD")
            assert spec.resolved_symbol == "EURUSD.a"
            assert abs(spec.spread_pips - 1.0) < 1e-6

            rates = client.get_rates("EURUSD", "M15", count=1)
            assert len(rates) == 1

            req = OrderRequest(
                external_id="ord-1",
                symbol="EURUSD",
                side="buy",
                volume=Decimal("0.50"),
                stop_loss=Decimal("1.08310"),
                take_profit=Decimal("1.08910"),
            )
            res = client.submit_order(req)
            assert res.accepted is True
            assert res.broker_ticket == "900123"

            dup = client.submit_order(req)
            assert dup.accepted is False
            assert dup.status == "duplicate"

            assert client.modify_position("900123", stop_loss=Decimal("1.08510")) is True
            assert client.close_partial("900123", Decimal("0.25")) is True
            assert client.close_all() == 1

            deals = client.get_closed_deals()
            assert len(deals) == 1
            assert deals[0].net_pnl == Decimal("196.50")

            ea.send_telemetry()
            for _ in range(20):
                if gw.latest_telemetry is not None:
                    break
                time.sleep(0.05)
            assert gw.latest_telemetry is not None
            assert gw.latest_telemetry.equity == Decimal("50120.00000")
        finally:
            ea.stop()


def test_cli_simulate_demo(capsys: pytest.CaptureFixture[str]) -> None:
    code = cli_main(["--port", "0", "--token", "t", "--simulate-demo", "--symbols", "EURUSD", "XAUUSD"])
    assert code == 0
    data = json.loads(capsys.readouterr().out)
    assert data["passed"] is True
    assert [s["resolved"] for s in data["symbols"]] == ["EURUSD.a", "XAUUSD.a"]
