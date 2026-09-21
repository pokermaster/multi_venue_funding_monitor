import asyncio
import json
import logging
import websockets
import asyncpg
from datetime import datetime, timezone
from engine import QuantitativeEngine

# ---------------------------------------------------------------------------
# 1. Logging Setup (File + Terminal)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("signals.log"),
        logging.StreamHandler()
    ]
)

# Initialize Quantitative Engine
quant_engine = QuantitativeEngine(
    spot_fee_rate=0.0007,    # 0.07% Spot Taker
    perp_fee_rate=0.00045,   # 0.045% Perp Maker/Taker
    borrow_apr=0.001,       # 0.10% Annual Borrow
    window_size=50          # Moving average window size
)

# Async Queue for Raw Market Ticks
tick_queue = asyncio.Queue()

# In-Memory Cache for Pair Alignment
market_state = {
    "spot": {"mid": None, "ts": None},
    "Binance-Perp": {"mid": None, "funding_rate": 0.0, "epochs_per_day": 3},
    "Hyperliquid": {"mid": None, "funding_rate": 0.0, "epochs_per_day": 24}
}

# ---------------------------------------------------------------------------
# 2. WebSocket Streams
# ---------------------------------------------------------------------------
async def binance_spot_stream(symbol="btcusdt"):
    url = f"wss://stream.binance.com:9443/ws/{symbol}@bookTicker"
    async with websockets.connect(url) as ws:
        logging.info(f"[System] Connected to Binance Spot {symbol.upper()}")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            await tick_queue.put({
                "type": "book", "exchange": "Binance-Spot", "symbol": "BTC",
                "bid": float(data.get("b", 0)), "ask": float(data.get("a", 0)),
                "ts": datetime.now(timezone.utc).replace(tzinfo=None)
            })

async def binance_perp_stream(symbol="btcusdt"):
    url = f"wss://fstream.binance.com/ws/{symbol}@bookTicker"
    async with websockets.connect(url) as ws:
        logging.info(f"[System] Connected to Binance Perp {symbol.upper()}")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            await tick_queue.put({
                "type": "book", "exchange": "Binance-Perp", "symbol": "BTC-PERP",
                "bid": float(data.get("b", 0)), "ask": float(data.get("a", 0)),
                "ts": datetime.now(timezone.utc).replace(tzinfo=None)
            })

async def binance_funding_stream(symbol="btcusdt"):
    url = f"wss://fstream.binance.com/ws/{symbol}@markPrice"
    async with websockets.connect(url) as ws:
        logging.info(f"[System] Connected to Binance Funding {symbol.upper()}")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            await tick_queue.put({
                "type": "funding", "exchange": "Binance-Perp", "symbol": "BTC-PERP",
                "funding_rate": float(data.get("r", 0)),
                "mark_price": float(data.get("p", 0)),
                "ts": datetime.now(timezone.utc).replace(tzinfo=None)
            })

async def hyperliquid_keepalive(ws):
    while True:
        await asyncio.sleep(50)
        try:
            await ws.send(json.dumps({"method": "ping"}))
        except websockets.exceptions.ConnectionClosed:
            break

async def hyperliquid_stream(coin="BTC"):
    url = "wss://api.hyperliquid.xyz/ws"
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}}))
        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": coin}}))
        
        logging.info(f"[System] Connected to Hyperliquid {coin} Book & Funding")
        ping_task = asyncio.create_task(hyperliquid_keepalive(ws))
        
        try:
            while True:
                msg = await ws.recv()
                data = json.loads(msg)
                channel = data.get("channel")
                
                if channel == "l2Book":
                    levels = data["data"]["levels"]
                    best_bid = float(levels[0][0]["px"]) if levels[0] else 0.0
                    best_ask = float(levels[1][0]["px"]) if levels[1] else 0.0
                    await tick_queue.put({
                        "type": "book", "exchange": "Hyperliquid", "symbol": f"{coin}-PERP",
                        "bid": best_bid, "ask": best_ask,
                        "ts": datetime.now(timezone.utc).replace(tzinfo=None)
                    })
                
                elif channel == "activeAssetCtx":
                    ctx = data["data"]["ctx"]
                    await tick_queue.put({
                        "type": "funding", "exchange": "Hyperliquid", "symbol": f"{coin}-PERP",
                        "funding_rate": float(ctx.get("funding", 0)),
                        "mark_price": float(ctx.get("markPx", 0)),
                        "ts": datetime.now(timezone.utc).replace(tzinfo=None)
                    })
        finally:
            ping_task.cancel()

# ---------------------------------------------------------------------------
# 3. QuestDB Storage & Signal Processing Worker
# ---------------------------------------------------------------------------
async def questdb_writer():
    conn = await asyncpg.connect(user="admin", password="quest", database="qdb", host="127.0.0.1", port=8812)
    
    # Tables: Orderbook, Funding, and Analytics Signals
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS orderbook_ticks (
            exchange SYMBOL, symbol SYMBOL, bid DOUBLE, ask DOUBLE, ts TIMESTAMP
        ) TIMESTAMP(ts) PARTITION BY DAY;
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS funding_ticks (
            exchange SYMBOL, symbol SYMBOL, funding_rate DOUBLE, mark_price DOUBLE, ts TIMESTAMP
        ) TIMESTAMP(ts) PARTITION BY DAY;
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS basis_signals (
            exchange SYMBOL, symbol SYMBOL, raw_basis DOUBLE, smoothed_basis DOUBLE,
            net_annual_yield DOUBLE, signal_active BOOLEAN, ts TIMESTAMP
        ) TIMESTAMP(ts) PARTITION BY DAY;
    """)
    logging.info("[System] QuestDB Writer Initialized on port 8812.")

    while True:
        tick = await tick_queue.get()
        ts = tick["ts"]

        if tick["type"] == "book":
            # 1. Store Raw Ticks
            await conn.execute(
                "INSERT INTO orderbook_ticks (exchange, symbol, bid, ask, ts) VALUES ($1, $2, $3, $4, $5)",
                tick["exchange"], tick["symbol"], tick["bid"], tick["ask"], ts
            )
            
            # 2. Update In-Memory Mid Prices
            mid = (tick["bid"] + tick["ask"]) / 2.0 if (tick["bid"] and tick["ask"]) else None
            if mid:
                if tick["exchange"] == "Binance-Spot":
                    market_state["spot"]["mid"] = mid
                    market_state["spot"]["ts"] = ts
                elif tick["exchange"] in market_state:
                    market_state[tick["exchange"]]["mid"] = mid

        elif tick["type"] == "funding":
            await conn.execute(
                "INSERT INTO funding_ticks (exchange, symbol, funding_rate, mark_price, ts) VALUES ($1, $2, $3, $4, $5)",
                tick["exchange"], tick["symbol"], tick["funding_rate"], tick["mark_price"], ts
            )
            if tick["exchange"] in market_state:
                market_state[tick["exchange"]]["funding_rate"] = tick["funding_rate"]

        # -------------------------------------------------------------------
        # 4. Quantitative Signal Evaluation
        # -------------------------------------------------------------------
        spot_price = market_state["spot"]["mid"]

        # Evaluate basis across connected perp venues if spot mid is available
        if spot_price and spot_price > 0:
            target_venues = [tick["exchange"]] if tick["exchange"] in ["Binance-Perp", "Hyperliquid"] else ["Binance-Perp", "Hyperliquid"]

            for venue in target_venues:
                perp_price = market_state[venue]["mid"]
                funding_rate = market_state[venue]["funding_rate"]
                epochs = market_state[venue]["epochs_per_day"]

                if perp_price and perp_price > 0:
                    try:
                        # Raw and moving-average basis calculation
                        raw_basis = quant_engine.calculate_basis_spread(perp_price, spot_price)
                        smoothed_basis = quant_engine.get_smoothed_basis(raw_basis)

                        # Net annualized carry yield calculation (30-day projection)
                        gross_yield = quant_engine.calculate_annualized_yield(funding_rate, epochs_per_day=epochs)
                        net_metrics = quant_engine.calculate_net_adjusted_yield(gross_yield, holding_period_days=30)
                        net_annual_yield = net_metrics["annualized_net_yield_pct"]

                        # Check entry threshold (e.g. basis > 0, net yield > 5%)
                        signal_triggered = quant_engine.evaluate_entry_signal(
                            smoothed_basis=smoothed_basis,
                            net_annual_yield=net_annual_yield,
                            min_yield_threshold=5.0
                        )

                        # Log alerts when actionable signals fire
                        if signal_triggered:
                            logging.warning(
                                f"🔥 ENTRY SIGNAL | Venue: {venue} | "
                                f"Smoothed Basis: {smoothed_basis:.2f} bps | "
                                f"Net Yield: {net_annual_yield:.2f}% | "
                                f"Spot: {spot_price:.2f} | Perp: {perp_price:.2f}"
                            )

                        # Persist quantitative calculation to QuestDB
                        await conn.execute(
                            "INSERT INTO basis_signals (exchange, symbol, raw_basis, smoothed_basis, net_annual_yield, signal_active, ts) "
                            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
                            venue, "BTC-PERP", raw_basis, smoothed_basis, net_annual_yield, signal_triggered, ts
                        )

                    except Exception as e:
                        logging.error(f"Error computing signals for {venue}: {e}")

        tick_queue.task_done()

# ---------------------------------------------------------------------------
# 4. Main Event Loop
# ---------------------------------------------------------------------------
async def main():
    await asyncio.gather(
        binance_spot_stream("btcusdt"),
        binance_perp_stream("btcusdt"),
        binance_funding_stream("btcusdt"),
        hyperliquid_stream("BTC"),
        questdb_writer()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Streams disconnected.")