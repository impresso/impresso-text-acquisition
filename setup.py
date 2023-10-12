"""Config for Pypi."""

import os
import pathlib
from setuptools import setup, find_packages
from text_importer import __version__

VERSION = __version__

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

setup(
    name='text_importer',
    author='Matteo Romanello',
    author_email='matteo.romanello@epfl.ch',
    url='https://github.com/impresso/impresso-text-acquisition',
    version=VERSION,
    packages=find_packages(),
    package_data={
        'text_importer': [
            'impresso-schemas/json/*/*.json',
            'impresso-schemas/docs/*/*.json',
            'impresso-schemas/*',
        ]
    },
    entry_points={
        'console_scripts': [
            'impresso-txt-importer = text_importer.importer:main',
            'impresso-txt-uploader = text_importer.upload:main',
        ]
    },
    python_requires='>=3.11',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],
    long_description=README,
    long_description_content_type='text/markdown',
    install_requires=[
        'beautifulsoup4',
        'docopt',
        'pandas',
        'dask[complete]',
        'lxml',
        'boto',
        'python-jsonschema-objects',
        'regex',
        'filelock',
    ],
)
