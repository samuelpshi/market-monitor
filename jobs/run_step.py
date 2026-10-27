"""Databricks spark_python_task entry point:
    python jobs/run_step.py --root <bundle files path> --step ingest

The job installs mdm from the bundle's wheel; --root is only a fallback for
running from a checkout (serverless can't import modules from workspace files,
and runs this file via exec(), where __file__ is undefined)."""

import os
import sys

args = sys.argv[1:]
root = None
if "--root" in args:
    i = args.index("--root")
    root = args[i + 1]
    del args[i:i + 2]

# Prefer the installed wheel over the bundle's workspace copy of mdm/.
if root:
    shadow = {os.path.normpath(root), os.path.normpath(os.path.join(root, "jobs"))}
    sys.path[:] = [p for p in sys.path if os.path.normpath(p or ".") not in shadow]

try:
    import mdm  # noqa: F401  (installed wheel)
except ImportError:
    if root is None:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) if "__file__" in globals() else os.getcwd()
    sys.path.insert(0, root)

from mdm.pipeline import main  # noqa: E402

main(args)
