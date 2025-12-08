"""
CVRmap cerebrovascular reactivity mapping tool.

This module provides CVRmap integration for ln2t_tools, supporting
cerebrovascular reactivity mapping from BOLD fMRI and physiological data.
"""

from .tool import CvrMapTool

# Export the tool class for automatic discovery
TOOL_CLASS = CvrMapTool

__all__ = ['CvrMapTool', 'TOOL_CLASS']
