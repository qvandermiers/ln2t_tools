"""
FastSurfer deep-learning based neuroimaging pipeline.

FastSurfer provides fast and accurate brain segmentation and surface
reconstruction using deep learning, offering a FreeSurfer-compatible
alternative with significantly reduced processing time.
"""

from .tool import FastSurferTool

# Required: export TOOL_CLASS for auto-discovery
TOOL_CLASS = FastSurferTool

__all__ = ['FastSurferTool', 'TOOL_CLASS']
