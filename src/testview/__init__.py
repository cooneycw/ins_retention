"""
TestView Module - Visual Debugger for PySpark Inforce Data

This module provides a tkinter-based GUI for exploring the inforce_monthly_view.csv
data with major change indicators, allowing navigation by policy and time.
"""

from .policy_viewer import PolicyViewer

__all__ = ['PolicyViewer']
