"""Structured logging.

Two formats from one call site: `text` for reading during a demo, `json` for
anything that has to be parsed. Extra fields travel in a single `extra={"ctx":
{...}}` dict so the JSON output carries real structure instead of an
already-formatted message string.

    log.info("row inserted", extra={"ctx": {"source": "naukri", "line": 2}})

    text -> 11:42:01 INFO  ingest  row inserted  source=naukri line=2
    json -> {"ts": "...", "level": "INFO", "msg": "row inserted",
             "source": "naukri", "line": 2}
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(getattr(record, "ctx", {}))
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        stamp = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        line = f"{stamp} {record.levelname:<5} {record.getMessage()}"
        ctx = getattr(record, "ctx", None)
        if ctx:
            line += "  " + " ".join(f"{k}={v}" for k, v in ctx.items())
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


def configure(level: str = "INFO", fmt: str = "text") -> logging.Logger:
    """Install a single stderr handler and return the pipeline logger.

    stderr, not stdout, so the CLI's human-facing summary can be piped or
    redirected without log lines mixed into it.
    """
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if fmt == "json" else TextFormatter())

    root = logging.getLogger("pipeline")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    root.propagate = False
    return root


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"pipeline.{name}")
