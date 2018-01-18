"""
Functions and CLI script to convert Olive OCR data into Impresso's format.

Usage:
    olive_importer.py --input-dir=<id> --output-dir==<od> [--log-file=<f>]
    olive_importer.py --input-dir=<id> --output-dir==<od> [--log-level=<l>]
"""

import os
import pdb
from docopt import docopt
from collections import namedtuple
from datetime import date

# TODO: use `dask` for parallel execution

__author__ = "Matteo Romanello"
__email__ = "matteo.romanello@epfl.ch"
__organisation__ = "impresso @ DH Lab, EPFL"
__copyright__ = "EPFL, 2017"
__status__ = "development"

# a simple data structure to represent input directories
# a `Document.zip` file is expected to be found in `IssueDir.path`
IssueDir = namedtuple(
    "IssueDirectory", [
        'journal',
        'date',
        'edition',
        'path'
    ]
)


# perhaps we don't need it
def to_canonical_filename(arg):
    pass


def import_issue(issue_dir, out_dir):
    """TODO.

    Program logic:
        - unzip the zip file in a temp directory (to be remove at the end)
        - parse each XML file and put in a data structure
        - serialize data structure to JSON, written in `out_dir` with canonical
            filename (but all files in one dir?, or divided by journal)
        - remove temporary directory

    """
    # or return (directory, False, error)
    return (issue_dir, True, None)


# by decoupling the directory parsing and the unzipping etc.
# this code can run in parallel
def detect_journal_issues(base_dir):
    """Parse a directory structure and detect newspaper issues to be imported.

    :param base_dir: the root of the directory structure
    """
    detected_issues = []
    known_journals = ["GDL", "EVT", "JDG", "LNQ"]  # TODO: anything to add?
    dir_path, dirs, files = next(os.walk(base_dir))
    journal_dirs = [d for d in dirs if d in known_journals]

    for journal in journal_dirs:
        journal_path = os.path.join(base_dir, journal)
        dir_path, year_dirs, files = next(os.walk(journal_path))
        # year_dirs = [d for d in dirs if len(d) == 4]

        for year in year_dirs:
            year_path = os.path.join(journal_path, year)
            dir_path, month_dirs, files = next(os.walk(year_path))

            for month in month_dirs:
                month_path = os.path.join(year_path, month)
                dir_path, day_dirs, files = next(os.walk(month_path))

                for day in day_dirs:
                    day_path = os.path.join(month_path, day)
                    # concerning `edition="a"`: for now, no cases of newspapers
                    # published more than once a day in Olive format (but it
                    # may come later on)
                    detected_issues.append(
                        IssueDir(
                            journal,
                            date(int(year), int(month), int(day)),
                            'a',
                            day_path
                        )
                    )

    return detected_issues


def main(args):
    """Execute the main with CLI parameters."""
    print(args)
    inp_dir = args["--input-dir"]
    journal_issues = detect_journal_issues(inp_dir)
    pdb.set_trace()
    result = [import_issue(i) for i in journal_issues]  # to be parallelized
    print(result)


if __name__ == '__main__':
    arguments = docopt(__doc__)
    main(arguments)
