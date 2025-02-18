..
    Copyright (C) 2018 CERN.
    cds-migrator-kit is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.

==================
 cds-migrator-kit
==================

Installation
============

Default Installation (without RDM or Videos)
---------------------------------------------
To install the package without RDM or videos, run:

.. code-block:: bash

    pip install .

Installation for RDM
----------------------
To install the package with RDM, run:

.. code-block:: bash

    pip install ".[rdm]"

To see available RDM commands, run:

.. code-block:: bash

    invenio migration --help

Installation for Videos
-----------------------
To install the package with cds-videos, run:

.. code-block:: bash

    pip install ".[videos]"

To see available videos commands, run:

.. code-block:: bash

    invenio migration videos --help

Running Tests Locally
=====================

For RDM
--------
Install rdm and test dependencies:

.. code-block:: bash

    pip install ".[rdm,tests]"


Run the rdm tests:

.. code-block:: bash

    ./run-tests.sh rdm

For Videos
----------
Install videos and test dependencies:

.. code-block:: bash

    pip install ".[videos,tests]"

Run the video tests:

.. code-block:: bash

    ./run-tests.sh videos


To run the interface:
=====================
.. code-block:: bash
    
    gunicorn -b :8080 --timeout 120 --graceful-timeout 60 cds_migrator_kit.app:app

