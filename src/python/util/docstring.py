#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/util/docstring.py#1 $
#
# Created 2006/09/12
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

#----------------------------------------------------------------------------
# getdoc
#----------------------------------------------------------------------------
def getdoc(x):
    """getdoc(x) --> docstring for x"""
    doc = getattr(x, '__doc__', None)
    if doc is None:
        return ''
    else:
        return str(doc)

#----------------------------------------------------------------------------
# summary
#----------------------------------------------------------------------------
def summary(x):
    """summary(x) --> first line of x.__doc__"""

    if not hasattr(x, '__doc__') or x.__doc__ is None:
        return ''
    
    # Get first line of docstring
    return getdoc(x).split('\n',1)[0].strip()

#----------------------------------------------------------------------------
# description
#----------------------------------------------------------------------------
def description(x):
    """description(x) --> [ remaining lines of x.__doc__ ]

    Returns a list docstring lines, with initial indent whitespace
    removed.  The first line of the docstring is omitted, as are any
    empty lines before the next non-empty line.
    """
    # Build lines of docstring, with initial indent removed
    out_lines = []

    # Get iterator over lines of docstring (skip first line)
    help_lines = iter(getdoc(x).split('\n')[1:])

    # Find initial non-empty line to get prefix length
    for line in help_lines:
        # Is line non-empty? (discard initial empty lines)
        if line.strip():
            prefixLen = len(line) - len(line.lstrip())
            out_lines.append(line.strip())
            break

    # Add remaining lines with prefix whitespace removed
    for line in help_lines:
        # If prefix is not all whitespace, strip as much as possible
        if line[:prefixLen].strip():
            out_lines.append(line.strip())
        else:
            out_lines.append(line[prefixLen:].rstrip())

    return out_lines
