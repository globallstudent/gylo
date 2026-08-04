import asyncio
import os
from pathlib import Path

import gylo

app = gylo.Gylo()

LEDGER = Path(os.environ["GYLO_CHAOS_LEDGER"])
DURATION = float(os.environ.get("GYLO_CHAOS_SLEEP_MS", "50")) / 1000


@app.task(name="record")
async def record(marker: int) -> None:
    """Append one line per execution so the harness can count duplicates.

    Opened per call and flushed immediately, because the process this runs in
    is expected to be killed without warning.
    """
    await asyncio.sleep(DURATION)
    with LEDGER.open("a") as ledger:
        ledger.write(f"{marker}\n")
        ledger.flush()
        os.fsync(ledger.fileno())
