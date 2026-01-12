"""
BIDS Validator tool.

This module provides BIDS dataset validation for ln2t_tools, using the
official bids-validator from the BIDS project.
"""

from .tool import BidsValidatorTool

# Export the tool class for automatic discovery
TOOL_CLASS = BidsValidatorTool

__all__ = ['BidsValidatorTool', 'TOOL_CLASS']
