#!/usr/bin/env python
#
# Copyright (C) 2005 Josh Taylor (Kosmix Corporation)
# Created 2005-07-28
# 
# This file is part of the util library.
# 
# The util library is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or any later
# version.
# 
# The util library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
# 
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

import math,time,re

def now():
    """now() --> timestamp

    Return the timestamp for the current time (in seconds from 1970/1/1)"""
    return time.time()

def dayPart(t):
    """dayPart(timestamp) --> timestamp

    Strip off the hours, minutes, and seconds from the input timestamp
    and return a timestamp for the given day at 00:00:00."""
    t = time.localtime(t)
    return time.mktime(t[:3]+(0,0,0)+t[6:])

def today():
    """today() --> timestamp

    Return timestamp for the current day at 00:00:00"""
    return dayPart(now())

_timescale = {
    's' : 1.0,
    'm' : 60.0,
    'h' : 3600.0,
    'd' : 86400.0,
    'w' : 604800.0,
    'y' : 31536000.0,
    }

def interpTime(hourStr, minStr, secStr, amPmStr):
    h = int(hourStr)
    m = minStr and int(minStr) or 0
    s = secStr and int(secStr) or 0
    if amPmStr is not None:
        amPmStr = amPmStr.lower()
        if h == 12 and amPmStr == 'a':
            h = 0
        elif h > 0 and h < 12 and amPmStr == 'p':
            h += 12
    return h * _timescale['h'] + m * _timescale['m'] + s

def interpDate(yearStr, monStr, dayStr):
    y = yearStr and int(yearStr) or time.localtime()[0]
    if y < 70:
        y += 2000
    elif y < 100:
        y += 1900

    m = int(monStr)
    d = int(dayStr)

    return time.mktime((y,m,d,0,0,0,0,0,-1))

_timepattern = r'(\d{1,2})(?::(\d{2}))?(?::(\d{2}))?(?:\s*(a|p)[m.]*)?'
_datepattern = r'(?:(\d{2,4})[-/])?(\d{1,2})[-/](\d{1,2})'

_timefmts = [
    (re.compile(r'now$',re.I), lambda x: now()),
    (re.compile(r'today$',re.I), lambda x: today()),
    (re.compile(r'yesterday$',re.I), lambda x: today() - _timescale['d']),
    (re.compile(r'tomorrow$',re.I), lambda x: today() + _timescale['d']),
    (re.compile(r'([-+]\s*\d+(?:\.\d+)?)\s*(s|m|h|d|w|y)\w*$',re.I),
     lambda x: now() + float(x.group(1)) * _timescale[x.group(2).lower()]),
    (re.compile(r'(\d+(?:\.\d+)?)\s*(s|m|h|d|w|y)\w* ago$',re.I),
     lambda x: now() - float(x.group(1)) * _timescale[x.group(2).lower()]),
    (re.compile(r'last (s|m|h|d|w|y)\w*$',re.I),
     lambda x: now() - _timescale[x.group(1).lower()]),
    (re.compile(r'next (s|m|h|d|w|y)\w*$',re.I),
     lambda x: now() + _timescale[x.group(1).lower()]),
    (re.compile(_timepattern + '$', re.I),
     lambda x: today() + interpTime(*x.group(1,2,3,4))),
    (re.compile(_datepattern + '$', re.I),
     lambda x: interpDate(*x.group(1,2,3))),
    (re.compile(_timepattern + r'\s+' + _datepattern + '$', re.I),
     lambda x: interpDate(*x.group(5,6,7)) + interpTime(*x.group(1,2,3,4))),
    (re.compile(_datepattern + r'\s+' + _timepattern + '$', re.I),
     lambda x: interpDate(*x.group(1,2,3)) + interpTime(*x.group(4,5,6,7))),
    ]    

def parseTime(s):
    """parseTime(str) --> timestamp

    Parse the given input string as a time and attempt to do the right
    thing.  Supports several formats, including:
       now          -- local time now
       today        -- today at 00:00:00
       yesterday    -- yesterday at 00:00:00
       tomorrow     -- tomorrow at 00:00:00
       +/-N UNIT    -- now +/- N UNITs, where N is some floating point
                       number and UNIT is one of seconds, minutes, hours,
                       days, weeks, months, or years (or some reasonable
                       abbreviation thereof)
       N UNIT ago   -- now - N UNITs, with N and UNIT as above
       last UNIT    -- now - 1 UNIT, with UNIT as above
       next UNIT    -- now + 1 UNIT, with UNIT as above
       TIME         -- today + TIME, where TIME can be H:MM:SS, H:MM, or
                       just H, either in 24 or 12-hr with AM/PM
       DATE         -- DATE at 00:00:00, where DATE can be M/D, YY/M/D, or
                       YYYY/M/D ('-' instead of '/' also works)
       DATE TIME    -- DATE at TIME, with DATE and TIME as above
       TIME DATE    -- DATE at TIME, with DATE and TIME as above
    """
    for pattern, interp in _timefmts:
        m = pattern.match(s)
        if m:
            return interp(m)
    raise ValueError('Unsupported date/time format: '+s)

#----------------------------------------------------------------------------
# main
#----------------------------------------------------------------------------
def main():
    import sys
    for x in sys.argv[1:]:
        try:
            t = time.ctime(parseTime(x))
        except ValueError, err:
            t = str(err)

        print x,'-->',t

if __name__ == '__main__':
    main()

