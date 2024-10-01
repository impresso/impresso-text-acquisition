import os
import pathlib
from contextlib import ExitStack
import logging
import pytest
import dask
from dask.distributed import Client

# global variables are imported from conftest.py
from impresso_essentials.io.s3 import read_s3_issues
from impresso_essentials.utils import get_pkg_resource

from text_preparation.rebuilders.rebuilder import rebuild_issues, compress

DASK_WORKERS_NUMBER = 8
DASK_MEMORY_LIMIT = "2G"
S3_CANONICAL_BUCKET = "s3://10-canonical-sandbox"
S3_REBUILT_BUCKET = "s3://20-rebuilt-sandbox"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# suppressing botocore's verbose logging
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("smart_open").setLevel(logging.WARNING)

log_file_mng = ExitStack()
log_dir = get_pkg_resource(log_file_mng, "data/logs/")
log_file = get_pkg_resource(log_file_mng, "data/logs/rebuilt_tests.log")
pathlib.Path(log_dir).mkdir(parents=True, exist_ok=True)

handler = logging.FileHandler(filename=log_file, mode="w")
formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
log_file_mng.close()

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
    client = None

elif DASK_SCHEDULER_STRATEGY == "external":
    client = Client("localhost:8686")
    print(f"Dask client {client}")

LIMIT_ISSUES = 10
test_data = [
    ("NZZ", 1897, LIMIT_ISSUES),
    ("JDG", 1830, LIMIT_ISSUES),
    ("JDG", 1862, LIMIT_ISSUES),
    ("GDL", 1806, LIMIT_ISSUES),
    ("IMP", 1994, LIMIT_ISSUES),
    ("luxzeit1858", 1858, LIMIT_ISSUES),
    ("indeplux", 1905, LIMIT_ISSUES),
    ("luxwort", 1860, LIMIT_ISSUES),
    ("buergerbeamten", 1909, LIMIT_ISSUES),
    ("FedGazDe", 1849, LIMIT_ISSUES),
    ("excelsior", 1911, LIMIT_ISSUES),
    ("oecaen", 1914, LIMIT_ISSUES),
]


@pytest.mark.parametrize("newspaper_id, year, limit", test_data)
def test_rebuild_solr(newspaper_id: str, year: int, limit: int):
    file_mng = ExitStack()

    input_bucket_name = S3_CANONICAL_BUCKET
    outp_dir = get_pkg_resource(file_mng, "data/rebuilt")

    input_issues = read_s3_issues(newspaper_id, year, input_bucket_name)
    print(f"{newspaper_id}/{year}: {len(input_issues)} issues to rebuild")
    print(f"limiting test rebuild to first {limit} issues.")

    issue_key, json_files = rebuild_issues(
        issues=input_issues[:limit],
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=client,
        _format="solr",
    )

    result = compress(issue_key, json_files, outp_dir)
    logger.info(result)
    assert result is not None
    file_mng.close()


def test_rebuild_for_passim():
    input_bucket_name = S3_CANONICAL_BUCKET
    file_mng = ExitStack()
    outp_dir = get_pkg_resource(file_mng, "data/rebuilt-passim")

    input_issues = read_s3_issues("luxwort", "1848", input_bucket_name)

    issue_key, json_files = rebuild_issues(
        issues=input_issues[:50],
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=client,
        _format="passim",
        filter_language=["fr"],
    )
    logger.info(f"%s: %s", issue_key, json_files)
    file_mng.close()
