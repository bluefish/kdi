#!/usr/bin/env python
#
# Copyright (C) 2006 Josh Taylor (Kosmix Corporation)
# Created 2006-03-16
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

import Queue

import threading
import os
import sys
_dryrun = False
_log = None

#----------------------------------------------------------------------------
# Option formatting
#----------------------------------------------------------------------------
def formatOpt(flag, arg):
    if arg:
        arg = str(arg).replace('\\','\\\\').replace('"','\\"')
        return '%s "%s"' % (flag, arg)
    elif arg is not None:
        return flag
    else:
        return ''

def composeOpts(opts):
    return ' '.join([formatOpt(k,v) for k,v in opts.items()])


#----------------------------------------------------------------------------
# CommandError
#----------------------------------------------------------------------------
class CommandError(RuntimeError):
    def __init__(self, cmd, status):
        RuntimeError.__init__(self, 'command failed (status=%r): %s' %
                              (status, cmd))

#----------------------------------------------------------------------------
# Command callbacks
#----------------------------------------------------------------------------
def reportError(cmd, ret, output):
    status = os.WEXITSTATUS(ret)
    signal = os.WTERMSIG(ret)
    info = repr(status)
    if signal:
        info += ', signal %r' % signal
    print >>sys.stderr, 'FAILED (%s): %s' % (info,cmd)
    if output:
        print >>sys.stderr, ' --> Output: %r' % output

def throwError(cmd, ret, output):
    status = os.WEXITSTATUS(ret)
    signal = os.WTERMSIG(ret)
    info = repr(status)
    if signal:
        info += ', signal %r' % signal
    raise CommandError('FAILED (%s) %r: %r' % (info,cmd,output))

def reportOk(cmd, ret, output):
    status = os.WEXITSTATUS(ret)
    signal = os.WTERMSIG(ret)
    info = repr(status)
    if signal:
        info += ', signal %r' % signal
    print >>sys.stderr, 'OK (%s): %s' % (info,cmd)
    if output:
        print >>sys.stderr, ' --> Output: %r' % output

#----------------------------------------------------------------------------
# CommandDispatcher
#----------------------------------------------------------------------------
class CommandDispatcher(object):
    def __init__(self, maxJobs, dryrun=None, log=None):
        self.pending = Queue.Queue()
        self.complete = Queue.Queue()
        self.threads = []
        self.outstanding = 0
        if dryrun is None:
            self.dryrun = _dryrun
        else:
            self.dryrun = dryrun
        if log is None:
            self.log = _log
        else:
            self.log = log
        for i in xrange(maxJobs):
            t = threading.Thread(target=self._threadLoop)
            t.setDaemon(True)
            self.threads.append(t)
            t.start()

    def _threadLoop(self):
        while True:
            # Get pending command
            cmd,retries = self.pending.get()

            # Short circuit if we're doing a dry run
            if self.dryrun:
                print cmd
                self.complete.put((cmd, 0, ''))
                continue

            # Log command
            if self.log:
                self.log(cmd)

            # Run command, collect output and return code
            fp = None
            try:
                if '2>' not in cmd:
                    fp = os.popen(cmd + ' 2>&1')
                else:
                    fp = os.popen(cmd)
                output = fp.read()
                ret = fp.close()
            except Exception, ex:
                if fp:
                    try:
                        fp.close()
                    except:
                        pass
                output = str(ex)
                ret = -1

            # Normalize return code
            if ret is None:
                ret = 0

            # If command failed, maybe retry
            if ret and retries > 0:
                # Log failure
                if self.log:
                    self.log('cmd failed (ret=%r), re-queuing: %r output=%r' %
                             (ret, cmd, output))
                # Put it back in run queue
                self.pending.put((cmd, retries-1))
            else:
                # Log failure
                if ret and self.log:
                    self.log('cmd failed (ret=%r): %r output=%r' %
                             (ret, cmd, output))
                # Put results in queue
                self.complete.put((cmd, ret, output))

    def addJob(self, cmd, retries=0):
        self.pending.put((cmd,retries))
        self.outstanding += 1

    def iterJobs(self):
        while self.outstanding:
            result = self.complete.get()
            self.outstanding -= 1
            yield result

    def __iter__(self):
        return self.iterJobs()

    def wait(self, okStatus=(0,), errCallback=None, okCallback=None):
        failed = 0
        for cmd,ret,output in self.iterJobs():
            status = os.WEXITSTATUS(ret)
            signal = os.WTERMSIG(ret)
            if signal or status not in okStatus:
                failed += 1
                if errCallback:
                    errCallback(cmd, ret, output)
            elif okCallback:
                okCallback(cmd, ret, output)
        return failed


#----------------------------------------------------------------------------
# System calls
#----------------------------------------------------------------------------
def system_nothrow(cmd):
    if _dryrun:
        print cmd
        return 0
    else:
        if _log:
            _log(cmd)
        status = os.system(cmd)
        return status

def system(cmd, retries=0, wait=90.0):
    while True:
        status = system_nothrow(cmd)
        if status == 0:
            return
        if retries <= 0:
            raise CommandError(cmd, status)
        retries -= 1
        if _log:
            _log("command failed: waiting %f sec..."%wait)
        import time
        time.sleep(wait)

#----------------------------------------------------------------------------
# Settings
#----------------------------------------------------------------------------
def setDryRun(isDry):
    global _dryrun
    _dryrun = isDry

def setLogCallback(log):
    global _log
    _log = log

def isDryRun():
    return _dryrun

#----------------------------------------------------------------------------
# Util
#----------------------------------------------------------------------------
def q(x):
    return '"%s"' % x.replace('\\','\\\\').replace('"','\\"')
