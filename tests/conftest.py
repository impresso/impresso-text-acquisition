import os
import pathlib
import dask
import logging
from dask.distributed import Client
from contextlib import ExitStack
from time import strftime
from impresso_essentials.utils import get_pkg_resource

DASK_WORKERS_NUMBER = 8
DASK_MEMORY_LIMIT = "1G"

# Use an env var to determine the type of dask scheduling to run:
# 1) synchronous; distributed external or distributed internal
try:
    DASK_SCHEDULER_STRATEGY = os.environ["PYTEST_DASK_SCHEDULER"]
except KeyError:
    DASK_SCHEDULER_STRATEGY = "internal"

if DASK_SCHEDULER_STRATEGY == "internal":
    client = Client(
        processes=False,
        n_workers=DASK_WORKERS_NUMBER,
        threads_per_worker=1,
        memory_limit=DASK_MEMORY_LIMIT,
    )
    print(f"Dask client {client}")
    print(f"Dask client {client.scheduler_info()['services']}")

elif DASK_SCHEDULER_STRATEGY == "synchronous":
    # it does not work perfectly but almost
    dask.config.set(scheduler="synchronous")

elif DASK_SCHEDULER_STRATEGY == "external":
    client = Client("localhost:8786")


logger = logging.getLogger()
logger.setLevel(logging.INFO)
f_mng = ExitStack()

log_dir = get_pkg_resource(f_mng, "data/logs", "text_preparation")
out_dir = get_pkg_resource(f_mng, "data/canonical_out/test_out/", "text_preparation")
temp_dir = get_pkg_resource(f_mng, "data/temp/", "text_preparation")
log_file = get_pkg_resource(f_mng, f"data/logs/test_logs/tests_{strftime('%Y-%m-%d_%H:%M')}.log", "text_preparation")
pathlib.Path(log_dir).mkdir(parents=True, exist_ok=True)
pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
pathlib.Path(temp_dir).mkdir(parents=True, exist_ok=True)

handler = logging.FileHandler(filename=log_file, mode="w")
formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
