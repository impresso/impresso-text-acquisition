"""Config for Pypi."""

import os
from setuptools import setup, find_packages

VERSION = ""


DESCRIPTION = """
TODO
"""

setup(
    name='text_importer',
    author='Matteo Romanello',
    author_email='matteo.romanello@epfl.ch',
    url='https://github.com/impresso/impresso-text-acquisition',
    version=VERSION,
    packages=find_packages(),
    package_data={
        'text_importer': [
            'data/*.*'
        ]
    },
    long_description=DESCRIPTION,
    install_requires=[
        'bs4',
        'docopt',
        'pandas',
        'dask[complete]',
        'lxml',
        'python-jsonschema-objects',
    ]
)
