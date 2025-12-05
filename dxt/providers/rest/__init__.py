"""REST API provider for DXT.

This provider enables extracting data from REST APIs.
"""

from dxt.providers.rest.connector import RestConnector
from dxt.providers.rest.extractor import RestExtractor

__all__ = ["RestConnector", "RestExtractor"]
