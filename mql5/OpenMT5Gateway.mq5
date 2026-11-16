//+------------------------------------------------------------------+
//|                                              OpenMT5Gateway.mq5  |
//|                    Open-Source Zero-DLL Cross-Platform MT5 Bridge|
//|                                       Copyright (c) 2026 MIT     |
//+------------------------------------------------------------------+
#property copyright "ICT Quant SaaS Contributors (MIT License)"
#property version   "1.00"
#property strict

#include <Trade\Trade.mqh>
#include <Trade\PositionInfo.mqh>

input string InpHost                = "127.0.0.1"; // Python OpenMT5Gateway host
input int    InpPort                = 18812;       // Python OpenMT5Gateway TCP port
input string InpAuthToken           = "";          // Shared auth token (optional)
input long   InpMagicNumber         = 202601;      // EA magic number
input ulong  InpDeviationPoints     = 20;          // Max slippage points
input int    InpPollIntervalMs      = 50;          // Socket poll interval (ms)
input int    InpTelemetryIntervalMs = 2000;        // Account telemetry interval (ms)

CTrade         g_trade;
CPositionInfo  g_pos;
int            g_socket = INVALID_HANDLE;
string         g_rxBuffer = "";
ulong          g_lastReconnectAttemptMs = 0;
ulong          g_lastTelemetryMs = 0;

string g_suffixes[] = {"", ".a", "m", ".raw", ".i", "_pro", ".pro", ".ecn"};

//+------------------------------------------------------------------+
int OnInit()
{
   g_trade.SetExpertMagicNumber(InpMagicNumber);
   g_trade.SetDeviationInPoints(InpDeviationPoints);
   ConnectGateway();
   EventSetMillisecondTimer(InpPollIntervalMs);
   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   EventKillTimer();
   DisconnectGateway();
}

//+------------------------------------------------------------------+
void OnTimer()
{
   if(g_socket == INVALID_HANDLE || !SocketIsConnected(g_socket))
   {
      ulong nowMs = GetTickCount64();
      if(nowMs - g_lastReconnectAttemptMs >= 2000)
      {
         g_lastReconnectAttemptMs = nowMs;
         ConnectGateway();
      }
      return;
   }

   ReadAvailableFrames();

   ulong nowMs = GetTickCount64();
   if(nowMs - g_lastTelemetryMs >= (ulong)InpTelemetryIntervalMs)
   {
      g_lastTelemetryMs = nowMs;
      SendTelemetry();
   }
}

//+------------------------------------------------------------------+
void ConnectGateway()
{
   DisconnectGateway();
   g_socket = SocketCreate();
   if(g_socket == INVALID_HANDLE)
   {
      PrintFormat("OpenMT5Gateway: SocketCreate failed err=%d", GetLastError());
      return;
   }
   if(!SocketConnect(g_socket, InpHost, (uint)InpPort, 1000))
   {
      PrintFormat("OpenMT5Gateway: Waiting for %s:%d (err=%d)", InpHost, InpPort, GetLastError());
      DisconnectGateway();
      return;
   }

   PrintFormat("OpenMT5Gateway: Connected to %s:%d", InpHost, InpPort);
   if(InpAuthToken != "")
   {
      string authFrame = StringFormat("{\"type\":\"AUTH\",\"token\":\"%s\"}", JsonEscape(InpAuthToken));
      SendJsonLine(authFrame);
   }
   SendTelemetry();
}

//+------------------------------------------------------------------+
void DisconnectGateway()
{
   if(g_socket != INVALID_HANDLE)
   {
      SocketClose(g_socket);
      g_socket = INVALID_HANDLE;
   }
   g_rxBuffer = "";
}

//+------------------------------------------------------------------+
bool SendJsonLine(const string json)
{
   if(g_socket == INVALID_HANDLE)
      return false;
   uchar bytes[];
   int len = StringToCharArray(json + "\n", bytes, 0, WHOLE_ARRAY, CP_UTF8);
   if(len <= 1)
      return false;
   // Exclude trailing NUL byte added by StringToCharArray
   int sent = SocketSend(g_socket, bytes, len - 1);
   if(sent < 0)
   {
      PrintFormat("OpenMT5Gateway: SocketSend failed err=%d", GetLastError());
      DisconnectGateway();
      return false;
   }
   return true;
}

//+------------------------------------------------------------------+
void ReadAvailableFrames()
{
   uint readable = SocketIsReadable(g_socket);
   while(readable > 0)
   {
      uchar buf[];
      uint toRead = MathMin(readable, (uint)8192);
      int read = SocketRead(g_socket, buf, toRead, 50);
      if(read <= 0)
         break;
      g_rxBuffer += CharArrayToString(buf, 0, read, CP_UTF8);
      readable = SocketIsReadable(g_socket);
   }

   int nlPos = StringFind(g_rxBuffer, "\n");
   while(nlPos >= 0)
   {
      string line = StringSubstr(g_rxBuffer, 0, nlPos);
      g_rxBuffer = StringSubstr(g_rxBuffer, nlPos + 1);
      StringTrimRight(line);
      StringTrimLeft(line);
      if(StringLen(line) > 0)
         HandleInboundLine(line);
      nlPos = StringFind(g_rxBuffer, "\n");
   }
}

//+------------------------------------------------------------------+
void HandleInboundLine(const string line)
{
   string msgType = ExtractJsonString(line, "type");
   if(msgType == "AUTH_ACK")
      return;
   if(msgType != "COMMAND")
      return;

   string reqId  = ExtractJsonString(line, "request_id");
   string action = ExtractJsonString(line, "action");

   if(action == "PING")
      SendJsonLine(StringFormat("{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":true,\"pong\":true}", reqId));
   else if(action == "GET_ACCOUNT")
      HandleGetAccount(reqId);
   else if(action == "GET_SYMBOL_SPEC")
      HandleGetSymbolSpec(reqId, line);
   else if(action == "GET_RATES")
      HandleGetRates(reqId, line);
   else if(action == "ORDER_SEND")
      HandleOrderSend(reqId, line);
   else if(action == "ORDER_MODIFY")
      HandleOrderModify(reqId, line);
   else if(action == "POSITION_CLOSE_PARTIAL")
      HandleClosePartial(reqId, line);
   else if(action == "CLOSE_ALL")
      HandleCloseAll(reqId, line);
   else if(action == "GET_CLOSED_DEALS")
      HandleGetClosedDeals(reqId, line);
   else
      SendJsonLine(StringFormat("{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":false,\"error\":\"unknown_action\"}", reqId));
}

//+------------------------------------------------------------------+
void SendTelemetry()
{
   double bal = AccountInfoDouble(ACCOUNT_BALANCE);
   double eq  = AccountInfoDouble(ACCOUNT_EQUITY);
   long   ts  = (long)TimeGMT();
   string frame = StringFormat(
      "{\"type\":\"TELEMETRY\",\"balance\":\"%.5f\",\"equity\":\"%.5f\",\"timestamp\":%lld,\"open_positions\":%d}",
      bal, eq, ts, PositionsTotal()
   );
   SendJsonLine(frame);
}

//+------------------------------------------------------------------+
void HandleGetAccount(const string reqId)
{
   long   login   = AccountInfoInteger(ACCOUNT_LOGIN);
   string server  = JsonEscape(AccountInfoString(ACCOUNT_SERVER));
   double balance = AccountInfoDouble(ACCOUNT_BALANCE);
   double equity  = AccountInfoDouble(ACCOUNT_EQUITY);
   long   srvTime = (long)TimeTradeServer();

   string resp = StringFormat(
      "{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":true,\"login\":\"%lld\",\"server\":\"%s\",\"balance\":\"%.5f\",\"equity\":\"%.5f\",\"server_time\":%lld}",
      reqId, login, server, balance, equity, srvTime
   );
   SendJsonLine(resp);
}

//+------------------------------------------------------------------+
string ResolveSymbol(const string requested)
{
   for(int i = 0; i < ArraySize(g_suffixes); i++)
   {
      string cand = requested + g_suffixes[i];
      if(SymbolSelect(cand, true))
      {
         double bid = SymbolInfoDouble(cand, SYMBOL_BID);
         if(bid > 0.0)
            return cand;
      }
   }
   return "";
}

//+------------------------------------------------------------------+
void HandleGetSymbolSpec(const string reqId, const string line)
{
   string reqSym   = ExtractJsonString(line, "symbol");
   string resolved = ResolveSymbol(reqSym);
   if(resolved == "")
   {
      SendJsonLine(StringFormat("{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":false,\"error\":\"symbol_not_found\"}", reqId));
      return;
   }

   double bid   = SymbolInfoDouble(resolved, SYMBOL_BID);
   double ask   = SymbolInfoDouble(resolved, SYMBOL_ASK);
   double point = SymbolInfoDouble(resolved, SYMBOL_POINT);
   int    digits = (int)SymbolInfoInteger(resolved, SYMBOL_DIGITS);
   double vmin  = SymbolInfoDouble(resolved, SYMBOL_VOLUME_MIN);
   double vmax  = SymbolInfoDouble(resolved, SYMBOL_VOLUME_MAX);
   double vstep = SymbolInfoDouble(resolved, SYMBOL_VOLUME_STEP);
   bool   allowed = (bool)TerminalInfoInteger(TERMINAL_TRADE_ALLOWED) &&
                    (SymbolInfoInteger(resolved, SYMBOL_TRADE_MODE) != SYMBOL_TRADE_MODE_DISABLED);

   string resp = StringFormat(
      "{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":true,\"resolved_symbol\":\"%s\",\"bid\":\"%.*f\",\"ask\":\"%.*f\",\"point\":\"%.*f\",\"digits\":%d,\"volume_min\":\"%.2f\",\"volume_max\":\"%.2f\",\"volume_step\":\"%.2f\",\"trade_allowed\":%s}",
      reqId, JsonEscape(resolved), digits, bid, digits, ask, digits, point, digits, vmin, vmax, vstep, (allowed ? "true" : "false")
   );
   SendJsonLine(resp);
}

//+------------------------------------------------------------------+
ENUM_TIMEFRAMES ParseTimeframe(const string tf)
{
   if(tf == "M1")  return PERIOD_M1;
   if(tf == "M5")  return PERIOD_M5;
   if(tf == "M15") return PERIOD_M15;
   if(tf == "M30") return PERIOD_M30;
   if(tf == "H1")  return PERIOD_H1;
   if(tf == "H4")  return PERIOD_H4;
   if(tf == "D1")  return PERIOD_D1;
   if(tf == "W1")  return PERIOD_W1;
   if(tf == "MN1") return PERIOD_MN1;
   return PERIOD_M15;
}

//+------------------------------------------------------------------+
void HandleGetRates(const string reqId, const string line)
{
   string reqSym   = ExtractJsonString(line, "symbol");
   string tfName   = ExtractJsonString(line, "timeframe");
   int    count    = (int)ExtractJsonNumber(line, "count", 200);
   string resolved = ResolveSymbol(reqSym);
   if(resolved == "")
   {
      SendJsonLine(StringFormat("{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":false,\"error\":\"symbol_not_found\"}", reqId));
      return;
   }

   MqlRates rates[];
   ArraySetAsSeries(rates, false);
   int copied = CopyRates(resolved, ParseTimeframe(tfName), 0, count, rates);
   if(copied <= 0)
   {
      SendJsonLine(StringFormat("{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":false,\"error\":\"copy_rates_failed\"}", reqId));
      return;
   }

   string arr = "[";
   for(int i = 0; i < copied; i++)
   {
      if(i > 0) arr += ",";
      arr += StringFormat(
         "{\"time\":%lld,\"open\":%.6f,\"high\":%.6f,\"low\":%.6f,\"close\":%.6f,\"volume\":%lld}",
         (long)rates[i].time, rates[i].open, rates[i].high, rates[i].low, rates[i].close, (long)rates[i].tick_volume
      );
   }
   arr += "]";
   SendJsonLine(StringFormat("{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":true,\"rates\":%s}", reqId, arr));
}

//+------------------------------------------------------------------+
bool SendOrderWithFillingFallback(
   const string symbol,
   const ENUM_ORDER_TYPE orderType,
   const double volume,
   const double price,
   const double sl,
   const double tp,
   const string comment,
   ulong &outTicket,
   double &outPrice
)
{
   ENUM_ORDER_TYPE_FILLING modes[3] = {ORDER_FILLING_IOC, ORDER_FILLING_FOK, ORDER_FILLING_RETURN};
   for(int m = 0; m < 3; m++)
   {
      g_trade.SetTypeFilling(modes[m]);
      bool ok = (orderType == ORDER_TYPE_BUY)
         ? g_trade.Buy(volume, symbol, price, sl, tp, comment)
         : g_trade.Sell(volume, symbol, price, sl, tp, comment);
      if(ok && (g_trade.ResultRetcode() == TRADE_RETCODE_DONE || g_trade.ResultRetcode() == TRADE_RETCODE_PLACED))
      {
         outTicket = g_trade.ResultOrder();
         outPrice  = g_trade.ResultPrice();
         return true;
      }
   }
   return false;
}

//+------------------------------------------------------------------+
void HandleOrderSend(const string reqId, const string line)
{
   string reqSym   = ExtractJsonString(line, "symbol");
   string side     = ExtractJsonString(line, "side");
   double volume   = ExtractJsonNumber(line, "volume", 0.01);
   double sl       = ExtractJsonNumber(line, "stop_loss", 0.0);
   double tp       = ExtractJsonNumber(line, "take_profit", 0.0);
   string comment  = ExtractJsonString(line, "comment");

   string resolved = ResolveSymbol(reqSym);
   if(resolved == "")
   {
      SendJsonLine(StringFormat("{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":false,\"error\":\"symbol_not_found\"}", reqId));
      return;
   }

   ENUM_ORDER_TYPE orderType = (side == "sell") ? ORDER_TYPE_SELL : ORDER_TYPE_BUY;
   double price = (orderType == ORDER_TYPE_BUY)
      ? SymbolInfoDouble(resolved, SYMBOL_ASK)
      : SymbolInfoDouble(resolved, SYMBOL_BID);

   ulong ticket = 0;
   double filledPrice = price;
   if(!SendOrderWithFillingFallback(resolved, orderType, volume, price, sl, tp, comment, ticket, filledPrice))
   {
      SendJsonLine(StringFormat(
         "{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":false,\"error\":\"retcode_%d_%s\"}",
         reqId, g_trade.ResultRetcode(), JsonEscape(g_trade.ResultRetcodeDescription())
      ));
      return;
   }

   SendJsonLine(StringFormat(
      "{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":true,\"ticket\":\"%llu\",\"filled_price\":\"%.6f\"}",
      reqId, ticket, filledPrice
   ));
}

//+------------------------------------------------------------------+
void HandleOrderModify(const string reqId, const string line)
{
   ulong  ticket = (ulong)StringToInteger(ExtractJsonString(line, "ticket"));
   double sl     = ExtractJsonNumber(line, "stop_loss", 0.0);
   double tp     = ExtractJsonNumber(line, "take_profit", 0.0);
   bool   ok     = g_trade.PositionModify(ticket, sl, tp);
   SendJsonLine(StringFormat("{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":%s}", reqId, (ok ? "true" : "false")));
}

//+------------------------------------------------------------------+
void HandleClosePartial(const string reqId, const string line)
{
   ulong  ticket = (ulong)StringToInteger(ExtractJsonString(line, "ticket"));
   double volume = ExtractJsonNumber(line, "volume", 0.0);
   bool   ok     = g_trade.PositionClosePartial(ticket, volume);
   SendJsonLine(StringFormat("{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":%s}", reqId, (ok ? "true" : "false")));
}

//+------------------------------------------------------------------+
void HandleCloseAll(const string reqId, const string line)
{
   int closed = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      if(g_pos.SelectByIndex(i))
      {
         if(g_pos.Magic() == InpMagicNumber || InpMagicNumber == 0)
         {
            if(g_trade.PositionClose(g_pos.Ticket()))
               closed++;
         }
      }
   }
   SendJsonLine(StringFormat("{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":true,\"closed_count\":%d}", reqId, closed));
}

//+------------------------------------------------------------------+
double FindPositionOpenPrice(const long posId, const double fallback)
{
   int total = HistoryDealsTotal();
   for(int i = 0; i < total; i++)
   {
      ulong d = HistoryDealGetTicket(i);
      if(d == 0) continue;
      if(HistoryDealGetInteger(d, DEAL_POSITION_ID) == posId &&
         HistoryDealGetInteger(d, DEAL_ENTRY) == DEAL_ENTRY_IN)
      {
         return HistoryDealGetDouble(d, DEAL_PRICE);
      }
   }
   return fallback;
}

//+------------------------------------------------------------------+
void HandleGetClosedDeals(const string reqId, const string line)
{
   long sinceTs = (long)ExtractJsonNumber(line, "since", 0.0);
   HistorySelect((datetime)sinceTs, TimeCurrent() + 60);

   int total = HistoryDealsTotal();
   string arr = "[";
   bool first = true;
   for(int i = 0; i < total; i++)
   {
      ulong dealTicket = HistoryDealGetTicket(i);
      if(dealTicket == 0) continue;
      if(HistoryDealGetInteger(dealTicket, DEAL_ENTRY) != DEAL_ENTRY_OUT) continue;
      if(InpMagicNumber != 0 && HistoryDealGetInteger(dealTicket, DEAL_MAGIC) != InpMagicNumber) continue;

      if(!first) arr += ",";
      first = false;

      string sym     = JsonEscape(HistoryDealGetString(dealTicket, DEAL_SYMBOL));
      long   dtype   = HistoryDealGetInteger(dealTicket, DEAL_TYPE);
      string side    = (dtype == DEAL_TYPE_SELL) ? "buy" : "sell"; // Closing SELL closes a BUY trade
      double vol     = HistoryDealGetDouble(dealTicket, DEAL_VOLUME);
      double closePx = HistoryDealGetDouble(dealTicket, DEAL_PRICE);
      double profit  = HistoryDealGetDouble(dealTicket, DEAL_PROFIT);
      double comm    = HistoryDealGetDouble(dealTicket, DEAL_COMMISSION);
      double swap    = HistoryDealGetDouble(dealTicket, DEAL_SWAP);
      long   posId   = HistoryDealGetInteger(dealTicket, DEAL_POSITION_ID);
      double openPx  = FindPositionOpenPrice(posId, closePx);
      long   closedT = HistoryDealGetInteger(dealTicket, DEAL_TIME);
      string comment = JsonEscape(HistoryDealGetString(dealTicket, DEAL_COMMENT));

      arr += StringFormat(
         "{\"ticket\":\"%lld\",\"symbol\":\"%s\",\"side\":\"%s\",\"volume\":\"%.2f\",\"open_price\":\"%.6f\",\"close_price\":\"%.6f\",\"profit\":\"%.2f\",\"commission\":\"%.2f\",\"swap\":\"%.2f\",\"closed_at\":%lld,\"comment\":\"%s\"}",
         posId, sym, side, vol, openPx, closePx, profit, comm, swap, closedT, comment
      );
   }
   arr += "]";
   SendJsonLine(StringFormat("{\"type\":\"RESPONSE\",\"request_id\":\"%s\",\"ok\":true,\"deals\":%s}", reqId, arr));
}

//+------------------------------------------------------------------+
// Minimal deterministic JSON helpers (pure MQL5, zero external includes)
string JsonEscape(const string s)
{
   string out = s;
   StringReplace(out, "\\", "\\\\");
   StringReplace(out, "\"", "\\\"");
   return out;
}

string ExtractJsonString(const string json, const string key)
{
   string pattern = "\"" + key + "\":\"";
   int start = StringFind(json, pattern);
   if(start < 0) return "";
   start += StringLen(pattern);
   int end = StringFind(json, "\"", start);
   if(end < 0) return "";
   return StringSubstr(json, start, end - start);
}

double ExtractJsonNumber(const string json, const string key, const double defaultVal)
{
   string pattern = "\"" + key + "\":";
   int start = StringFind(json, pattern);
   if(start < 0) return defaultVal;
   start += StringLen(pattern);
   int endComma = StringFind(json, ",", start);
   int endBrace = StringFind(json, "}", start);
   int end = endComma;
   if(end < 0 || (endBrace >= 0 && endBrace < end)) end = endBrace;
   if(end < 0) return defaultVal;
   string raw = StringSubstr(json, start, end - start);
   StringTrimLeft(raw);
   StringTrimRight(raw);
   return StringToDouble(raw);
}
//+------------------------------------------------------------------+
