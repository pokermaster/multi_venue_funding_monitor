"""Compatibility entry point. See funding_monitor/ for the pipeline implementation."""
import sys

if sys.version_info < (3, 11):
    raise SystemExit("Python 3.11+ is required. Run: python3.11 feed_handler.py --mode demo")

from funding_monitor.app import main

if __name__ == "__main__":
    main()
