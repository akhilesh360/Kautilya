import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class PaperTradingService:
    """File-based paper trading audit log (JSONL, one file per UTC day)."""

    def __init__(self, base_dir: str = "data/paper_trading"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _day_path(self, date_str: Optional[str] = None) -> str:
        day = date_str or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return os.path.join(self.base_dir, f"audit_{day}.jsonl")

    def log_event(self, event_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        record = {
            "timestamp": now.isoformat(),
            "eventType": event_type,
            "payload": payload or {},
        }
        path = self._day_path(now.strftime("%Y-%m-%d"))
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=True) + "\n")
        return {"status": "ok", "path": path, "record": record}

    def read_logs(self, date_str: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        path = self._day_path(date_str)
        records: List[Dict[str, Any]] = []
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        continue
        if limit and limit > 0:
            records = records[-limit:]
        return {"date": date_str, "path": path, "count": len(records), "records": records}
