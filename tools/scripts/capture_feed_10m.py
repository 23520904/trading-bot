"""Capture Coinbase live WebSocket matches feed for 10 minutes (600s) and record stats on sequence vs trade_id continuity."""

import asyncio
import json
import time
from pathlib import Path

import websockets

OUTPUT_FILE = Path("tools/scripts/coinbase_10m_stats.json")


async def capture() -> None:
    url = "wss://ws-feed.exchange.coinbase.com"
    duration = 600.0  # 10 minutes
    start_time = time.time()
    end_time = start_time + duration

    trade_ids: list[int] = []
    sequences: list[int] = []
    reconnect_count = 0
    message_count = 0

    print(f"Starting 10-minute capture from {url} at {time.ctime(start_time)}...", flush=True)

    while time.time() < end_time:
        try:
            # ping_interval=20, ping_timeout=15 to prevent false keepalive timeouts
            async with websockets.connect(url, ping_interval=20, ping_timeout=15) as ws:
                sub_msg = {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["matches"]}
                await ws.send(json.dumps(sub_msg))
                print(f"Subscribed to matches at {time.ctime(time.time())}. Waiting for trades...", flush=True)

                last_log = time.time()
                while time.time() < end_time:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        obj = json.loads(raw)
                        if obj.get("type") == "match":
                            message_count += 1
                            t_id = int(obj["trade_id"])
                            seq = int(obj["sequence"])
                            trade_ids.append(t_id)
                            sequences.append(seq)

                        now = time.time()
                        if now - last_log >= 60.0:
                            elapsed = now - start_time
                            print(
                                f"[{elapsed:.0f}s / {duration:.0f}s] Captured {message_count} trades. "
                                f"Latest trade_id={trade_ids[-1] if trade_ids else None}",
                                flush=True,
                            )
                            last_log = now
                    except TimeoutError:
                        continue
        except Exception as exc:
            reconnect_count += 1
            print(f"Connection dropped ({type(exc).__name__}: {exc}). Reconnecting in 1s...", flush=True)
            await asyncio.sleep(1.0)

    total_time = time.time() - start_time
    trade_id_diffs = [trade_ids[i + 1] - trade_ids[i] for i in range(len(trade_ids) - 1)]
    seq_diffs = [sequences[i + 1] - sequences[i] for i in range(len(sequences) - 1)]

    trade_id_gaps = [d for d in trade_id_diffs if d != 1]
    seq_gaps = [d for d in seq_diffs if d != 1]

    stats = {
        "duration_seconds": total_time,
        "total_matches": message_count,
        "reconnect_count": reconnect_count,
        "trade_id": {
            "first": trade_ids[0] if trade_ids else None,
            "last": trade_ids[-1] if trade_ids else None,
            "total_gaps": len(trade_id_gaps),
            "max_gap": max(trade_id_diffs) if trade_id_diffs else 0,
            "min_gap": min(trade_id_diffs) if trade_id_diffs else 0,
            "gap_details": [
                {"idx": i, "from": trade_ids[i], "to": trade_ids[i + 1], "diff": trade_id_diffs[i]}
                for i in range(len(trade_id_diffs))
                if trade_id_diffs[i] != 1
            ],
            "contiguous_pct": (len(trade_ids) - 1 - len(trade_id_gaps)) / max(1, len(trade_ids) - 1) * 100.0,
        },
        "sequence": {
            "first": sequences[0] if sequences else None,
            "last": sequences[-1] if sequences else None,
            "total_gaps": len(seq_gaps),
            "max_gap": max(seq_diffs) if seq_diffs else 0,
            "min_gap": min(seq_diffs) if seq_diffs else 0,
            "gap_details_sample": [
                {"idx": i, "from": sequences[i], "to": sequences[i + 1], "diff": seq_diffs[i]}
                for i in range(min(10, len(seq_diffs)))
                if seq_diffs[i] != 1
            ],
            "contiguous_pct": (len(sequences) - 1 - len(seq_gaps)) / max(1, len(sequences) - 1) * 100.0,
        },
    }

    OUTPUT_FILE.write_text(json.dumps(stats, indent=2))
    print(f"Capture completed in {total_time:.1f}s. Total matches: {message_count}", flush=True)
    print(
        f"Trade ID gaps: {len(trade_id_gaps)} (Contiguity: {stats['trade_id']['contiguous_pct']:.2f}%)",
        flush=True,
    )
    print(
        f"Sequence gaps: {len(seq_gaps)} (Contiguity: {stats['sequence']['contiguous_pct']:.2f}%)",
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(capture())
