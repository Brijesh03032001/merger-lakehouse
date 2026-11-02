"""Execute Databricks-source notebooks (``# COMMAND ----------`` format) locally."""

from __future__ import annotations

import contextlib
import io
import shutil
import sys
import time
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

# Open-source Spark's built-in catalog is named spark_catalog, so the notebooks'
# ``{catalog}.{schema}.{table}`` identifiers resolve without Unity Catalog.
LOCAL_CATALOG = "spark_catalog"

_CELL_SEPARATOR = "# COMMAND ----------"
_NOTEBOOK_HEADER = "# Databricks notebook source"


@dataclass
class _FileInfo:
    path: str
    name: str
    size: int


def _local(path: str) -> Path:
    return Path(path.removeprefix("file:").removeprefix("dbfs:"))


class _FS:
    def ls(self, path: str) -> list[_FileInfo]:
        directory = _local(path)
        if not directory.is_dir():
            raise FileNotFoundError(f"java.io.FileNotFoundException: {path}")
        return [
            _FileInfo(str(item), item.name + ("/" if item.is_dir() else ""), item.stat().st_size)
            for item in sorted(directory.iterdir())
        ]

    def mv(self, source: str, target: str, recurse: bool = False) -> bool:
        destination = _local(target)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(_local(source)), str(destination))
        return True

    def mkdirs(self, path: str) -> bool:
        _local(path).mkdir(parents=True, exist_ok=True)
        return True


class _Widgets:
    def __init__(self, parameters: Mapping[str, str]) -> None:
        self._parameters = dict(parameters)
        self._defaults: dict[str, str] = {}

    def text(self, name: str, default: str = "", label: str | None = None) -> None:
        self._defaults[name] = default

    def get(self, name: str) -> str:
        return str(self._parameters.get(name, self._defaults[name]))

    def removeAll(self) -> None:
        self._defaults.clear()


class _Dbutils:
    def __init__(self, parameters: Mapping[str, str]) -> None:
        self.widgets = _Widgets(parameters)
        self.fs = _FS()


@dataclass
class NotebookResult:
    namespace: dict[str, Any]
    seconds: float
    cell_seconds: list[tuple[int, float, str]] = field(default_factory=list)
    stdout: str = ""


def init_local_catalog(spark, catalog: str = LOCAL_CATALOG) -> None:
    """Create the medallion schemas that ``1_setup/setup_catalogs.py`` creates on Databricks."""
    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def _cells(source: str) -> list[str]:
    body = source.removeprefix(_NOTEBOOK_HEADER + "\n")
    return body.split(_CELL_SEPARATOR)


def _run_source(path: Path, namespace: dict[str, Any], result: NotebookResult, *, top_level: bool) -> None:
    for index, cell in enumerate(_cells(path.read_text(encoding="utf-8"))):
        lines = [line for line in cell.splitlines() if line.strip()]
        if lines and lines[0].startswith("# MAGIC"):
            command = lines[0].removeprefix("# MAGIC").strip()
            if command.startswith("%run"):
                target = (path.parent / command.removeprefix("%run").strip()).resolve()
                if not target.suffix:
                    target = target.with_suffix(".py")
                _run_source(target, namespace, result, top_level=False)
            elif command.startswith(("%sql", "%pip", "%sh")):
                raise NotImplementedError(f"{path.name}: unsupported magic {command!r}")
            continue  # %md and other markdown-only cells
        started = time.perf_counter()
        exec(compile(cell, f"{path.name}[cell {index}]", "exec"), namespace)
        if top_level:
            first = lines[0].strip() if lines else ""
            result.cell_seconds.append((index, time.perf_counter() - started, first[:80]))


def run_notebook(
    path: str | Path,
    *,
    spark,
    parameters: Mapping[str, str] | None = None,
    quiet: bool = True,
) -> NotebookResult:
    """Run one notebook and return its final namespace, timing and captured stdout."""
    notebook = Path(path).resolve()
    dbutils = _Dbutils(parameters or {})

    def display(frame, *_, **__):
        frame.limit(20).collect()

    # A real module, as on Databricks: @dataclass looks up sys.modules[__name__] to resolve annotations.
    module = types.ModuleType("__notebook__")
    sys.modules["__notebook__"] = module
    namespace: dict[str, Any] = module.__dict__
    namespace.update(spark=spark, dbutils=dbutils, display=display)
    result = NotebookResult(namespace=namespace, seconds=0.0)
    buffer = io.StringIO()
    started = time.perf_counter()
    context = contextlib.redirect_stdout(buffer) if quiet else contextlib.nullcontext()
    with context:
        _run_source(notebook, namespace, result, top_level=True)
    result.seconds = time.perf_counter() - started
    result.stdout = buffer.getvalue()
    return result
