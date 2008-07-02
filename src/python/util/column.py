#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/util/column.py#1 $
#
# Created 2006/09/11
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

#----------------------------------------------------------------------------
# fitFields
#----------------------------------------------------------------------------
def fitFields(items, fields=None, width=70, label=False):
    if not items:
        return iter('')

    if fields is None:
        try:
            fields = [x for x in dir(items[0]) if not x.startswith('_')]
        except:
            fields = []

    nFields = len(fields)
    attrs = []
    labels = []
    for x in fields:
        try:
            attr,lbl = x
            lbl = str(lbl)
        except ValueError:
            attr = x
            lbl = str(x)
        attrs.append(attr)
        labels.append(lbl)

    values = []
    for x in items:
        values.append([str(x[attr]) for attr in attrs])

    fwidths = [max([len(v[i]) for v in values]) for i in range(nFields)]
    hdr = None
    if label:
        fwidths = [max(fwidths[i], len(labels[i])) for i in range(nFields)]
        hdr = [ ' '.join([l.ljust(w) for l,w in zip(labels,fwidths)]),
                ' '.join(['-'*w for w in fwidths]) ]

    args = [ ' '.join([v.ljust(w) for v,w in zip(row,fwidths)])
             for row in values ]
    return fitColumns(args, width=width, sep='  |  ', header=hdr)


#----------------------------------------------------------------------------
# printFields
#----------------------------------------------------------------------------
def printFields(items, fields=None, width=70, label=False):
    for line in fitFields(items, fields, width, label):
        print line


#----------------------------------------------------------------------------
# fitColumns
#----------------------------------------------------------------------------
def fitColumns(args, width=70, sep='  ', header=None):
    # Get lengths of arguments
    nItems = len(args)
    seplen = len(sep)
    lens = [len(x) for x in args]

    # Start with a number of columns equal to the number of items,
    # then iteratively find the best number of columns to use.
    height = 0
    cols = nItems
    colWidths = [0]

    # Bail out if we're down to one column
    while cols >= 1:

        # Compute number of rows
        height = int((nItems + cols - 1) / cols)

        # Find max width for each column
        colWidths = [max(lens[c*height:(c+1)*height]) for c in range(cols)]

        # Get total width of all columns and separators
        totalWidth = sum(colWidths) + (cols - 1) * seplen

        # If current layout fits, we're done
        if totalWidth <= width:
            break

        # Reduce number of columns and try again.  Either increase
        # number of rows by one or decrease number of columns by one,
        # whichever results in fewer columns.
        cols = min(int((nItems + height) / (height + 1)), cols - 1)

    # Don't pad the last column
    colWidths[-1] = 0

    # Emit header
    if header:
        for line in header:
            yield sep.join([line for i in range(cols)])

    # Emit each row
    for r in range(height):
        yield sep.join([x.ljust(w) for x,w in zip(args[r::height], colWidths)])


#----------------------------------------------------------------------------
# printColumns
#----------------------------------------------------------------------------
def printColumns(args, width=70, sep='  ', header=None):
    for row in fitColumns(args, width, sep):
        print row
