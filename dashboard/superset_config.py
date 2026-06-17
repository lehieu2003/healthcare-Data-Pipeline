from __future__ import annotations

import os


SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "local-dev-change-me")

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

SQLLAB_CTAS_NO_LIMIT = True
