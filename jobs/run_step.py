"""Databricks spark_python_task entry point: python jobs/run_step.py --step ingest"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mdm.pipeline import main  # noqa: E402

if __name__ == "__main__":
    main(sys.argv[1:])
