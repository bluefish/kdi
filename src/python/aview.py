#!/usr/bin/env python
#
# $Id: //depot/SOURCE/TASKS/josh.crawler/src/python/aview.py#1 $
#
# Created 2006/03/29
#
# Copyright 2006 Cosmix Corporation.  All rights reserved.
# Cosmix PROPRIETARY and CONFIDENTIAL.

import sys
import alien
import alien.tools
#from alien.common import *
from util import bag
from util.xmlfmt import xml, qqescape

def main():
    layouts = {
        'compact'        : alien.tools.LAYOUT_COMPACT,
        'normal'         : alien.tools.LAYOUT_NORMAL,
        'extended'       : alien.tools.LAYOUT_EXTENDED,
        'super-extended' : alien.tools.LAYOUT_SUPER_EXTENDED,
        }
    
    import optparse
    op = optparse.OptionParser()
    op.add_option('-t', '--type', metavar='T', help='Type of data to display')
    op.add_option('-v', '--version', metavar='V', type='int',
                  help='Version of data type')
    op.add_option('-r', '--record', action='store_true',
                  help='Set data type to Kosmix Record')
    op.add_option('-s', '--skip', type='long', metavar='N', default=0,
                  help='Skip N bytes before reading')
    op.add_option('-S', '--skiprecords', type='int', metavar='N', default=0,
                  help='Skip N records before printing')
    op.add_option('-n', '--count', type='int', metavar='N',
                  help='Read N records')
    op.add_option('-B', '--bufsize', type='int', default=(1<<20), metavar='SZ',
                  help='Buffer size for reading ahead')
    op.add_option('-m', '--minsize', type='int', default=(1<<10), metavar='SZ',
                  help='Minimum buffer size')
    op.add_option('-b', '--brief', action='store_true',
                  help='Omit data headers')
    op.add_option('-l', '--layout', metavar='L', default='normal',
                  choices=layouts.keys(),
                  help='Layout style for normal output')
    op.add_option('-w', '--width', type='int', metavar='N',
                  help='Formatting width for normal output')
    op.add_option('-x', '--xform', metavar='X', default='',
                  help="Python expression on data 'x' (e.g. 'x.length' or"
                  " 'str(x).lower().strip()'")
    op.add_option('-X', '--xml', action='store_true', help='Output in XML')
    op.add_option('-F', '--nofix', action='store_true',
                  help="Don't precompute offsets for variable-sized types")
    opt,args = op.parse_args()

    if not args:
        args = ['-']

    if opt.record:
        opt.type = 'alien.kosmix.record.Record'
        if not opt.xform:
            opt.xform = 'x.data'
    if not opt.type:
        op.error('need a type')

    # Get type
    sym = {}
    exec 'from alien.common import *' in sym
    #print sym.keys()

    asym = {}
    exec 'from alien import *' in asym
    #print asym.keys()
    
    ksym = {}
    exec 'from alien.kosmix import *' in ksym
    #print ksym.keys()
    
    asym['kosmix'] = bag.Bag(**ksym)
    sym['alien'] = bag.Bag(**asym)

    try:
        dtype = eval(opt.type, sym)
    except Exception,ex:
        print >>sys.stderr, 'Unable to get type: %s' % opt.type
        print >>sys.stderr, '  %s: %s' % (ex.__class__.__name__, ex)
        sys.exit(1)

    # Get size
    dsize = dtype._sizeof(opt.version)
    if callable(dsize):
        fsize = dsize
    else:
        fsize = lambda x: dsize

    # Get skip
    skip = opt.skip

    # Get skiprecords
    skiprecords = opt.skiprecords
    if skiprecords < 0:
        if callable(dsize):
            op.error('cannot skip backwards with variable size records')
        if skip > 0:
            skip += dsize * skiprecords
            if skip < 0:
                op.error('cannot seek before start of file')
        else:
            skip += dsize * skiprecords
        skiprecords = 0
    elif skiprecords > 0:
        if not callable(dsize):
            if skip < 0:
                skip += dsize * skiprecords
                if skip >= 0:
                    op.error('cannot seek past end of file')
            else:
                skip += dsize * skiprecords
            skiprecords = 0

    # Make data transform
    if opt.xform:
        try:
            xform = eval('lambda x: '+opt.xform, sym)
        except Exception,ex:
            print >>sys.stderr, 'Unable to process transform: %s' % opt.xform
            print >>sys.stderr, '  %s: %s' % (ex.__class__.__name__, ex)
            sys.exit(1)
    else:
        xform = lambda x: x

    try:

        # Start XML
        if opt.xml:
            print '<?xml version="1.0"?>'
            print '<aview>'
        
        # Read the data
        for fn in args:
        
            # Track position
            pos = 0                         # Record position in file
        
            # Open next file
            if fn == '-':
                # It's stdin
                f = sys.stdin
        
                # Seek to starting pos
                if skip > 0:
                    while pos < skip:
                        sz = len(f.read(min(skip - pos, 16<<10)))
                        if not sz:
                            break
                        pos += sz
                elif skip < 0:
                    op.error('cannot skip from end of stdin')
        
            else:
                # It's a regular file
                f = file(fn)
        
                # Seek to starting pos
                if skip > 0:
                    f.seek(skip)
                    pos = f.tell()
                elif skip < 0:
                    f.seek(skip, 2)
                    pos = f.tell()
        
            # Buffer position
            buf = f.read(opt.bufsize)       # Deserialization buffer
            base = 0                        # Record position in buffer
        
            # Prep counts
            nonprinting = skiprecords
            if opt.count is not None:
                maxItems = opt.count + skiprecords
            else:
                maxItems = -1
            nItems = 0
        
            # Read stuff
            while True:
        
                # Read until we've hit maxItems (or forever, if maxItems
                # is negative)
                if nItems == maxItems:
                    break
        
                # Read more if buffer does not have at least minSize data
                # remaining
                if base >= len(buf) - opt.minsize:
                    more = f.read(opt.bufsize)
                    if more:
                        buf = buf[base:] + more
                        base = 0
                        del more
        
                # Done yet?
                if base >= len(buf):
                    break
        
                # Deserialize a record
                x = dtype(buf, base, opt.version)

                # Fix offsets
                if hasattr(x, '_fixoffsets') and not opt.nofix:
                   x._fixoffsets()

                # Get size (it will be faster AFTER fixing offsets)
                sz = fsize(x)
        
                # Make sure we have the whole record before continuing
                if base + sz > len(buf):
                    more = f.read(max(base, sz))
                    if len(buf) + len(more) - base >= sz:
                        buf = buf[base:] + more
                        base = 0
                        del more
                        x = dtype(buf, base, opt.version)

                        # Fix offsets
                        if hasattr(x, '_fixoffsets') and not opt.nofix:
                            x._fixoffsets()

                        # Check that size hasn't grown
                        assert sz <= fsize(x)
                    else:
                        raise IOError('short read: needed %d bytes, got %d'
                                      % (sz, len(buf) + len(more) - base))
        
                # We have a record
                nItems += 1
        
                # Display this record (unless we're counting down
                # non-printing records)
                if not nonprinting:
        
                    # Transform data
                    try:
                        xv = xform(x)
                    except Exception, ex:
                        print 'Unable to apply transform: %s' % opt.xform
                        print '  %s: %s' % (ex.__class__.__name__, ex)
                        sys.exit(1)
        
                    # Display
                    if opt.xml:
                        print '<item file="%s" offset="%s">%s</item>' % \
                              (qqescape(f.name), qqescape(str(pos)), xml(xv))
                    else:
                        if not opt.brief:
                            print '%s %d (%s:%d): %s'%(x, nItems, f.name, pos, opt.xform)
                        for line in alien.tools.iter_format_struct(xv, layouts[opt.layout], opt.width):
                            print line
                        if not opt.brief:
                            print
        
                else:
                    # Non-printing item; count down
                    nonprinting -= 1
        
                # Advance position pointers
                pos += sz
                base += sz
        
            # And close it up
            f.close()
        
        # End XML
        if opt.xml:
            print '</aview>'

    except IOError, ex:
        if ex.errno == 32:
            sys.exit(1)
        raise


if __name__ == '__main__':
    main()
