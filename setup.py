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
    package_data={
        'text_importer': [
            'data/',
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
        'ipdb',  # TODO: remove from production
        'impresso_commons',
        'pandas',
        'dask[complete]',
        'lxml',
        'boto',
        'python-jsonschema-objects',
        'regex'
    ],
    dependency_links=[
      'https://github.com/impresso/impresso-pycommons/tarball/master#egg=impresso_commons',
      ]
)
