"""
Modernized pytopspeed library for reading Clarion TopSpeed files (.tps and .phd)
"""

from .tps import TPS, topread

__version__ = "1.1.2"
__all__ = ["TPS", "topread"]