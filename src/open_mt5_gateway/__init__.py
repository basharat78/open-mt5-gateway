"""OpenMT5Gateway — Open-Source Cross-Platform MetaTrader 5 Bridge."""

from open_mt5_gateway.gateway import (
    AccountInfo,
    ClosedDeal,
    GatewayConfig,
    OpenMT5Client,
    OpenMT5GatewayServer,
    OrderRequest,
    OrderResult,
    SymbolSpec,
    TelemetrySnapshot,
)

__all__ = [
    "AccountInfo",
    "ClosedDeal",
    "GatewayConfig",
    "OpenMT5Client",
    "OpenMT5GatewayServer",
    "OrderRequest",
    "OrderResult",
    "SymbolSpec",
    "TelemetrySnapshot",
]
__version__ = "0.1.1"
