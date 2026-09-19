import asyncio
import json
import websockets
import asyncpg
from datetime import datetime, timezone

# 1. Initialize async queue
tick_queue = asyncio.Queue()

async def binance_ticker_stream(symbol="btcusdt"):
    """Connects to Binance USDS-M Futures for real-time top-of-book updates."""
    url=f"wss://fstream.binance.com/ws/{symbol}@bookTicker"

    async with websockets.connect(url) as ws:
        print(f"[System] Connected to Binance {symbol.upper()} Perps")
        while True:
            msg = await ws.recv()
            data = json.loads(msg)

            # 2. Push data to the queue immediately (Producer)
            await tick_queue.put({
                "exchange": "Binance",
                "symbol": "BTC-PERP",
                "bid": float(data.get("b", 0)),
                "ask": float(data.get("a", 0)),
                "ts": datetime.now(timezone.utc).replace(tzinfo=None) 
            })

async def hyperliquid_keepalive(ws):
    """ sends ping to avoid disconnection """
    while True:
        await asyncio.sleep(50)
        try:
            await ws.send(json.dumps({"method":"ping"}))
        except websockets.exceptions.ConnectionClosed:
            break 

async def hyperliquid_l2_stream(coin="BTC"):
    """Connects to Hyperliquid L1 for real-time L2 order book updates."""
    url = "wss://api.hyperliquid.xyz/ws"

    async with websockets.connect(url) as ws:
        subscribe_msg = {
            "method" : "subscribe",
            "subscription" : {"type": "l2Book", "coin": coin}
        }
        await ws.send(json.dumps(subscribe_msg))
        print(f"[System] Connected to Hyperliquid {coin} Perps")

        ping_task = asyncio.create_task(hyperliquid_keepalive(ws))

        try:
            while True:
                msg = await ws.recv()
                data = json.loads(msg)

                if data.get("channel") == "l2Book":
                    levels = data["data"]["levels"]
                    # levels[0] contains bids, levels[1] contains asks  
                    best_bid = float(levels[0][0]["px"]) if levels[0] else 0.0
                    best_ask = float(levels[1][0]["px"]) if levels[1] else 0.0

                    # 2. Push data to the queue immediately (Producer)
                    await tick_queue.put({
                        "exchange": "Hyperliquid",
                        "symbol": "BTC-PERP",
                        "bid": best_bid,
                        "ask": best_ask,
                        "ts": datetime.now(timezone.utc).replace(tzinfo=None)
                    })

        finally:
            ping_task.cancel()

async def questdb_writer():
    """Consumer task that writes queued ticks to QuestDB."""
    conn = await asyncpg.connect(
        user="admin",
        password="quest",
        database="qdb",
        host="127.0.0.1",
        port=8812
    )

    # 3. Create a time-series optimized schema using QuestDB's SYMBOL type
    await conn.execute("""
    CREATE TABLE IF NOT EXISTS orderbook_ticks (
            exchange SYMBOL,
            symbol SYMBOL,
            bid DOUBLE,
            ask DOUBLE,
            ts TIMESTAMP
        ) TIMESTAMP(ts) PARTITION BY DAY;
    """)
    print("[System] QuestDB Writer Initialized on port 8812.")

    while True:
        # 4. Pull the next tick from the queue and insert it
        tick = await tick_queue.get()

        await conn.execute("""
        INSERT INTO orderbook_ticks (exchange, symbol, bid, ask, ts) 
            VALUES ($1, $2, $3, $4, $5)
        """, tick["exchange"], tick["symbol"], tick["bid"], tick["ask"], tick["ts"])

        # Mark the task as processed
        tick_queue.task_done()

async def main():
    # 5. Run the producers and the consumer concurrently    
    await asyncio.gather(
        binance_ticker_stream(symbol="btcusdt"),
        hyperliquid_l2_stream(coin="BTC"),
        questdb_writer()
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Streams disconnected.")
        