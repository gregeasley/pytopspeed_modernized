"""
Modernized pytopspeed library for reading Clarion TopSpeed files (.tps and .phd)
"""

from .tps import TPS, topread

__version__ = "1.1.0"
__all__ = ["TPS", "topread"]