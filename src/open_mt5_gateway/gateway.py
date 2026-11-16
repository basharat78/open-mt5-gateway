"""Standalone, zero-dependency reverse-TCP gateway and Python client for MetaTrader 5."""

from __future__ import annotations

import hmac
import json
import socket
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class GatewayConfig:
    """Configuration for `OpenMT5GatewayServer`."""

    host: str = "127.0.0.1"
    port: int = 18812
    auth_token: str = ""
    request_timeout_seconds: float = 5.0
    magic_number: int = 202601
    deviation_points: int = 20

    def __post_init__(self) -> None:
        if self.port < 0 or self.port > 65535:
            raise ValueError("port must be in 0..65535")
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")


@dataclass(frozen=True)
class AccountInfo:
    """MT5 account snapshot returned by `GET_ACCOUNT`."""

    login: str
    server: str
    balance: Decimal
    equity: Decimal
    server_time_utc: datetime


@dataclass(frozen=True)
class SymbolSpec:
    """Broker symbol specification and live quote returned by `GET_SYMBOL_SPEC`."""

    requested_symbol: str
    resolved_symbol: str
    bid: Decimal
    ask: Decimal
    point: Decimal
    digits: int
    volume_min: Decimal
    volume_max: Decimal
    volume_step: Decimal
    trade_allowed: bool = True

    @property
    def spread_pips(self) -> float:
        upper = self.resolved_symbol.upper()
        pip = Decimal("0.01") if "JPY" in upper else Decimal("0.0001")
        if "XAU" in upper or "GOLD" in upper:
            pip = Decimal("0.10")
        return float((self.ask - self.bid) / pip)


@dataclass(frozen=True)
class OrderRequest:
    """Market order instruction sent to MT5."""

    external_id: str
    symbol: str
    side: str  # "buy" | "sell"
    volume: Decimal
    stop_loss: Decimal = Decimal("0")
    take_profit: Decimal | None = None
    comment: str = ""

    def __post_init__(self) -> None:
        if self.side not in {"buy", "sell"}:
            raise ValueError("side must be 'buy' or 'sell'")
        if self.volume <= 0:
            raise ValueError("volume must be positive")


@dataclass(frozen=True)
class OrderResult:
    """Execution result returned by MT5."""

    accepted: bool
    broker_ticket: str | None
    status: str  # "open" | "rejected_broker" | "duplicate"
    filled_price: Decimal | None = None
    reject_reason: str | None = None
    executed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class ClosedDeal:
    """Historical closed deal returned by `GET_CLOSED_DEALS`."""

    broker_ticket: str
    symbol: str
    side: str
    volume: Decimal
    open_price: Decimal
    close_price: Decimal
    profit: Decimal
    commission: Decimal = Decimal("0")
    swap: Decimal = Decimal("0")
    closed_at: datetime | None = None
    comment: str = ""

    @property
    def net_pnl(self) -> Decimal:
        return self.profit + self.commission + self.swap


@dataclass(frozen=True)
class TelemetrySnapshot:
    """Live equity/balance telemetry streamed asynchronously by the EA."""

    balance: Decimal
    equity: Decimal
    open_positions: int
    captured_at: datetime


class OpenMT5GatewayServer:
    """Thread-safe NDJSON TCP server accepting connections from `OpenMT5Gateway.mq5`."""

    def __init__(self, config: GatewayConfig | None = None) -> None:
        self.config = config or GatewayConfig()
        self._server_sock: socket.socket | None = None
        self._client_sock: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()
        self._send_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._pending: dict[str, list[Any]] = {}
        self._latest_telemetry: dict[str, Any] | None = None
        self._bound_port: int = self.config.port

    @property
    def bound_port(self) -> int:
        return self._bound_port

    @property
    def is_connected(self) -> bool:
        return self._connected_event.is_set()

    @property
    def latest_telemetry(self) -> TelemetrySnapshot | None:
        with self._state_lock:
            raw = dict(self._latest_telemetry) if self._latest_telemetry is not None else None
        if raw is None:
            return None
        ts = raw.get("timestamp")
        return TelemetrySnapshot(
            balance=Decimal(str(raw["balance"])),
            equity=Decimal(str(raw["equity"])),
            open_positions=int(raw.get("open_positions", 0)),
            captured_at=(
                datetime.fromtimestamp(int(ts), tz=timezone.utc)
                if ts is not None
                else datetime.now(timezone.utc)
            ),
        )

    def start(self) -> int:
        """Bind TCP socket and start accepting EA connections in a daemon thread."""
        if self._server_sock is not None:
            return self._bound_port
        self._stop_event.clear()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.settimeout(0.25)
        srv.bind((self.config.host, self.config.port))
        srv.listen(4)
        self._bound_port = int(srv.getsockname()[1])
        self._server_sock = srv
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            name="open-mt5-gateway-accept",
            daemon=True,
        )
        self._accept_thread.start()
        return self._bound_port

    def stop(self) -> None:
        """Stop the gateway and cleanly unblock any pending commands."""
        self._stop_event.set()
        self._connected_event.clear()
        with self._state_lock:
            client = self._client_sock
            self._client_sock = None
            pending_items = list(self._pending.values())
            self._pending.clear()
        for entry in pending_items:
            entry[1] = {"ok": False, "error": "gateway_stopped"}
            entry[0].set()
        if client is not None:
            try:
                client.close()
            except OSError:
                pass
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except OSError:
                pass
            self._server_sock = None
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2.0)
            self._accept_thread = None

    def __enter__(self) -> OpenMT5GatewayServer:
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()

    def wait_for_connection(self, timeout: float = 10.0) -> bool:
        """Wait until an authenticated MQL5 EA connects or `timeout` seconds pass."""
        return self._connected_event.wait(timeout=timeout)

    def _accept_loop(self) -> None:
        while not self._stop_event.is_set() and self._server_sock is not None:
            try:
                conn, _addr = self._server_sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            self._handle_client(conn)

    def _handle_client(self, conn: socket.socket) -> None:
        conn.settimeout(0.5)
        reader = conn.makefile("r", encoding="utf-8", errors="replace")
        authenticated = self.config.auth_token == ""
        if authenticated:
            self._activate_client(conn)

        try:
            while not self._stop_event.is_set():
                try:
                    line = reader.readline()
                except TimeoutError:
                    continue
                if not line:
                    break
                text = line.strip()
                if not text:
                    continue
                try:
                    msg = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if not isinstance(msg, dict):
                    continue

                if not authenticated:
                    if msg.get("type") == "AUTH" and hmac.compare_digest(
                        str(msg.get("token", "")),
                        self.config.auth_token,
                    ):
                        authenticated = True
                        self._write_json_line(conn, {"type": "AUTH_ACK", "ok": True})
                        self._activate_client(conn)
                    else:
                        self._write_json_line(
                            conn,
                            {"type": "AUTH_ACK", "ok": False, "error": "invalid_token"},
                        )
                        break
                    continue

                self._dispatch_inbound(msg)
        except OSError:
            pass
        finally:
            self._deactivate_client(conn)
            for res in (reader, conn):
                try:
                    res.close()
                except OSError:
                    pass

    def _activate_client(self, conn: socket.socket) -> None:
        with self._state_lock:
            old = self._client_sock
            self._client_sock = conn
        if old is not None and old is not conn:
            try:
                old.close()
            except OSError:
                pass
        self._connected_event.set()

    def _deactivate_client(self, conn: socket.socket) -> None:
        with self._state_lock:
            if self._client_sock is conn:
                self._client_sock = None
                self._connected_event.clear()
                pending_items = list(self._pending.values())
                self._pending.clear()
            else:
                pending_items = []
        for entry in pending_items:
            entry[1] = {"ok": False, "error": "client_disconnected"}
            entry[0].set()

    def _dispatch_inbound(self, msg: dict[str, Any]) -> None:
        msg_type = msg.get("type")
        if msg_type == "TELEMETRY":
            with self._state_lock:
                self._latest_telemetry = dict(msg)
            return

        req_id = msg.get("request_id")
        if msg_type == "RESPONSE" and isinstance(req_id, str):
            with self._state_lock:
                entry = self._pending.pop(req_id, None)
            if entry is not None:
                entry[1] = msg
                entry[0].set()

    @staticmethod
    def _write_json_line(conn: socket.socket, payload: dict[str, Any]) -> None:
        raw = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
        conn.sendall(raw)

    def send_command(
        self,
        action: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Send an RPC command to MT5 EA and wait for the matching response."""
        req_id = uuid4().hex
        waiter: list[Any] = [threading.Event(), None]
        frame = {
            "type": "COMMAND",
            "request_id": req_id,
            "action": action,
            "payload": payload or {},
        }

        with self._state_lock:
            conn = self._client_sock
            if conn is None:
                raise ConnectionError("No OpenMT5Gateway MQL5 EA client is connected.")
            self._pending[req_id] = waiter

        try:
            with self._send_lock:
                self._write_json_line(conn, frame)
        except OSError as exc:
            with self._state_lock:
                self._pending.pop(req_id, None)
            raise ConnectionError(f"Failed sending command to MT5 EA: {exc}") from exc

        eff_timeout = timeout or self.config.request_timeout_seconds
        if not waiter[0].wait(timeout=eff_timeout):
            with self._state_lock:
                self._pending.pop(req_id, None)
            raise TimeoutError(f"OpenMT5Gateway timeout waiting for {action} response.")

        resp = waiter[1]
        if not isinstance(resp, dict):
            raise ConnectionError("Invalid response payload received from MT5 EA.")
        return resp


class OpenMT5Client:
    """High-level Python client over `OpenMT5GatewayServer`."""

    def __init__(
        self,
        gateway: OpenMT5GatewayServer,
        *,
        magic_number: int | None = None,
        deviation_points: int | None = None,
    ) -> None:
        self.gateway = gateway
        self.magic_number = magic_number or gateway.config.magic_number
        self.deviation_points = deviation_points or gateway.config.deviation_points
        self._submitted_external_ids: set[str] = set()
        self._lock = threading.Lock()

    def ping(self) -> bool:
        resp = self.gateway.send_command("PING")
        return bool(resp.get("ok") and resp.get("pong"))

    def get_account(self) -> AccountInfo:
        resp = self.gateway.send_command("GET_ACCOUNT")
        if not resp.get("ok"):
            raise RuntimeError(f"GET_ACCOUNT failed: {resp.get('error', 'unknown')}")
        srv_ts = resp.get("server_time")
        return AccountInfo(
            login=str(resp["login"]),
            server=str(resp["server"]),
            balance=Decimal(str(resp["balance"])),
            equity=Decimal(str(resp["equity"])),
            server_time_utc=(
                datetime.fromtimestamp(int(srv_ts), tz=timezone.utc)
                if srv_ts is not None
                else datetime.now(timezone.utc)
            ),
        )

    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        resp = self.gateway.send_command("GET_SYMBOL_SPEC", {"symbol": symbol})
        if not resp.get("ok"):
            raise RuntimeError(f"GET_SYMBOL_SPEC failed for {symbol}: {resp.get('error')}")
        return SymbolSpec(
            requested_symbol=symbol,
            resolved_symbol=str(resp["resolved_symbol"]),
            bid=Decimal(str(resp["bid"])),
            ask=Decimal(str(resp["ask"])),
            point=Decimal(str(resp["point"])),
            digits=int(resp["digits"]),
            volume_min=Decimal(str(resp["volume_min"])),
            volume_max=Decimal(str(resp["volume_max"])),
            volume_step=Decimal(str(resp["volume_step"])),
            trade_allowed=bool(resp.get("trade_allowed", True)),
        )

    def get_rates(
        self,
        symbol: str,
        timeframe: str = "M15",
        count: int = 200,
    ) -> list[dict[str, Any]]:
        """Return OHLCV bar dicts (`time`, `open`, `high`, `low`, `close`, `volume`)."""
        resp = self.gateway.send_command(
            "GET_RATES",
            {"symbol": symbol, "timeframe": timeframe, "count": int(count)},
        )
        if not resp.get("ok"):
            raise RuntimeError(f"GET_RATES failed: {resp.get('error', 'unknown')}")
        return list(resp.get("rates", []))

    def submit_order(self, request: OrderRequest) -> OrderResult:
        with self._lock:
            if request.external_id in self._submitted_external_ids:
                return OrderResult(
                    accepted=False,
                    broker_ticket=None,
                    status="duplicate",
                    reject_reason="duplicate external_id",
                )

        comment = (request.comment or request.external_id)[:31]
        try:
            resp = self.gateway.send_command(
                "ORDER_SEND",
                {
                    "external_id": request.external_id,
                    "symbol": request.symbol,
                    "side": request.side,
                    "volume": float(request.volume),
                    "stop_loss": float(request.stop_loss),
                    "take_profit": float(request.take_profit) if request.take_profit is not None else 0.0,
                    "magic": self.magic_number,
                    "deviation": self.deviation_points,
                    "comment": comment,
                },
            )
        except (ConnectionError, TimeoutError) as exc:
            return OrderResult(
                accepted=False,
                broker_ticket=None,
                status="rejected_broker",
                reject_reason=str(exc),
            )

        if not resp.get("ok"):
            return OrderResult(
                accepted=False,
                broker_ticket=None,
                status="rejected_broker",
                reject_reason=str(resp.get("error", "order_rejected")),
            )

        with self._lock:
            self._submitted_external_ids.add(request.external_id)

        filled = resp.get("filled_price")
        return OrderResult(
            accepted=True,
            broker_ticket=str(resp["ticket"]),
            status="open",
            filled_price=Decimal(str(filled)) if filled is not None else None,
        )

    def modify_position(
        self,
        broker_ticket: str,
        *,
        stop_loss: Decimal,
        take_profit: Decimal | None = None,
    ) -> bool:
        resp = self.gateway.send_command(
            "ORDER_MODIFY",
            {
                "ticket": str(broker_ticket),
                "stop_loss": float(stop_loss),
                "take_profit": float(take_profit) if take_profit is not None else 0.0,
            },
        )
        return bool(resp.get("ok"))

    def close_partial(self, broker_ticket: str, volume: Decimal) -> bool:
        resp = self.gateway.send_command(
            "POSITION_CLOSE_PARTIAL",
            {
                "ticket": str(broker_ticket),
                "volume": float(volume),
                "deviation": self.deviation_points,
            },
        )
        return bool(resp.get("ok"))

    def close_all(self, reason: str = "manual_close_all") -> int:
        resp = self.gateway.send_command(
            "CLOSE_ALL",
            {"reason": reason, "magic": self.magic_number},
        )
        if not resp.get("ok"):
            return 0
        return int(resp.get("closed_count", 0))

    def get_closed_deals(self, since_timestamp: int = 0) -> list[ClosedDeal]:
        resp = self.gateway.send_command(
            "GET_CLOSED_DEALS",
            {"since": int(since_timestamp), "magic": self.magic_number},
        )
        if not resp.get("ok"):
            raise RuntimeError(f"GET_CLOSED_DEALS failed: {resp.get('error', 'unknown')}")

        deals: list[ClosedDeal] = []
        for item in resp.get("deals", []):
            closed_ts = item.get("closed_at")
            deals.append(
                ClosedDeal(
                    broker_ticket=str(item["ticket"]),
                    symbol=str(item["symbol"]),
                    side=str(item["side"]),
                    volume=Decimal(str(item["volume"])),
                    open_price=Decimal(str(item["open_price"])),
                    close_price=Decimal(str(item["close_price"])),
                    profit=Decimal(str(item["profit"])),
                    commission=Decimal(str(item.get("commission", "0"))),
                    swap=Decimal(str(item.get("swap", "0"))),
                    closed_at=(
                        datetime.fromtimestamp(int(closed_ts), tz=timezone.utc)
                        if closed_ts is not None
                        else None
                    ),
                    comment=str(item.get("comment", "")),
                )
            )
        return deals
