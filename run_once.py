"""Manually trigger a single pipeline run right now (incremental window).

Usage:
    python run_once.py
"""
from __future__ import annotations

from pipeline.runner import run_pipeline

if __name__ == "__main__":
    run_pipeline()
