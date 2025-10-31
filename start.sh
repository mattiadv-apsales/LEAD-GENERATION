#!/bin/bash
# installa Chromium per Playwright
python -m playwright install chromium
# avvia Flask tramite Gunicorn
gunicorn server:app --bind 0.0.0.0:$PORT --workers 1 --threads 4
