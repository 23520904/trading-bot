"""Root pytest configuration for CIP.

Ensures CIP_ENV is defaulted to 'test' during test execution.
"""

import os

os.environ.setdefault("CIP_ENV", "test")
