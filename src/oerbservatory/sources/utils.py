"""Utilities and constants."""

from pathlib import Path

__all__ = ["OUTPUT_DIR"]

HERE = Path(__file__).parent.resolve()
ROOT = HERE.parent.parent.resolve()
OUTPUT_DIR = ROOT.joinpath("output")
OUTPUT_DIR.mkdir(exist_ok=True)
