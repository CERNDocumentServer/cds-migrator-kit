..
    Copyright (C) 2018 CERN.
    cds-migrator-kit is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.

==================
 cds-migrator-kit
==================

To run the interface:
```
gunicorn -b :8080 --timeout 120 --graceful-timeout 60 cds_migrator_kit.app:app
```
