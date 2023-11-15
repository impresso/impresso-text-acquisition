Installation
============

For now you'll have to:

.. code-block:: bash

  # use --recursive as the main repo contains a git sub-module
  git clone --recursive https://github.com/impresso/impresso-text-acquisition.git

  cd impresso-text-acquisition

  # install dependencies into your virtual environment
  pip install -r requirements.txt

  # install the TextImporter
  pip install -e .
