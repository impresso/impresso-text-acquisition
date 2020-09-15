"""Config for Pypi."""

import os
from setuptools import setup, find_packages
from text_importer import __version__

VERSION = __version__


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
    include_package_data=True,
    package_data={
        'text_importer': [
            'data/', 'data/tests.log', 'impresso-schemas/json/newspaper/*.json'
        ]
    },
    entry_points={
        'console_scripts': [
            'impresso-txt-importer = text_importer.importer:main',
            'impresso-txt-uploader = text_importer.upload:main',
        ]
    },
    long_description=DESCRIPTION,
    install_requires=[
        'bs4',
        'docopt',
        'impresso_commons',
        'dask_k8',
        'pandas',
        'dask[complete]',
        'lxml',
        'boto',
        'python-jsonschema-objects',
        'regex'
    ],
    dependency_links=[
      'https://github.com/impresso/impresso-pycommons/tarball/master#egg=impresso_commons-0.12.4',
      'https://github.com/impresso/dask_k8/tarball/master#egg=dask_k8-0.1.1',
      ]
)
