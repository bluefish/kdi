#!/usr/bin/env python
#
# Copyright (C) 2009 Josh Taylor (Kosmix Corporation)
# Created 2009-05-29
#
# This file is part of KDI.
#
# KDI is free software; you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or any later version.
#
# KDI is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.


import threading
import Queue

#----------------------------------------------------------------------------
# States
#----------------------------------------------------------------------------
TABLE_UNKNOWN = 0
TABLE_SCHEMA_LOADING = 1
TABLE_ACTIVE = 2

TABLET_UNKNOWN = 0
TABLET_CONFIG_LOADING = 1
TABLET_LOG_REPLAYING = 2
TABLET_CONFIG_SAVING = 3
TABLET_FRAGMENTS_LOADING = 4
TABLET_ACTIVE = 5
TABLET_UNLOAD_COMPACTING = 6
TABLET_UNLOAD_CONFIG_SAVING = 7

#----------------------------------------------------------------------------
# name util
#----------------------------------------------------------------------------
def decodeTabletName(x):
    n = len(x) + 1
    i = min(x.find(' ') % n,
            x.find('!') % n)
    return x[:i], x[i:]

#----------------------------------------------------------------------------
# Schema
#----------------------------------------------------------------------------
class Schema:
    class Group:
        def __init__(self):
            self.families = []

    def __init__(self):
        self.tableName = ''
        self.groups = []

    def __str__(self):
        return 'Schema(%s)' % self.tableName

#----------------------------------------------------------------------------
# Config
#----------------------------------------------------------------------------
class Config:
    class Fragment:
        def __init__(self):
            self.filename = ''
            self.families = []
    
    def __init__(self):
        self.tableName = ''
        self.lastRow = ''
        self.fragments = []
        self.log = ''
        self.location = ''

    def empty(self):
        return not (self.fragments or self.log or self.location)

    def __str__(self):
        return 'Config(%s%s)' % (self.tableName, self.lastRow)

#----------------------------------------------------------------------------
# Tablet
#----------------------------------------------------------------------------
class Tablet:
    def __init__(self):
        self.state = TABLET_CONFIG_LOADING
        self.loadcb = []

    def getState(self):
        return self.state

    def deferUntilLoaded(self, cb):
        self.loadcb.append(cb)

    def applyConfig(self, config):
        print 'Config applied: %s' % config
        self.state = TABLET_LOG_REPLAYING
        for x in self.loadcb:
            x.done()

#----------------------------------------------------------------------------
# Table
#----------------------------------------------------------------------------
class Table:
    def __init__(self):
        self.state = TABLE_SCHEMA_LOADING
        self.tablets = {}

    def applySchema(self, schema):
        print 'Schema applied: %s' % schema
        self.state = TABLE_ACTIVE

    def getState(self):
        return self.state

#----------------------------------------------------------------------------
# QueuedWorker
#----------------------------------------------------------------------------
class QueuedWorker(threading.Thread):
    def __init__(self):
        super(QueuedWorker,self).__init__()
        self._q = Queue.Queue()
        self.daemon = True

    def run(self):
        while True:
            work = self._q.get()
            self.doWork(work)
            self._q.task_done()

    def submit(self, work):
        self._q.put(work)

#----------------------------------------------------------------------------
# SchemaLoaderWork
#----------------------------------------------------------------------------
class SchemaLoaderWork:
    def __init__(self, cb, tableName):
        self.cb = cb
        self.tableName = tableName

#----------------------------------------------------------------------------
# SchemaLoader
#----------------------------------------------------------------------------
class SchemaLoader(QueuedWorker):
    def __init__(self, server):
        super(SchemaLoader, self).__init__()
        self.server = server

    def doWork(self, work):
        x = Schema()
        x.tableName = work.tableName

        print 'Schema loaded: %s' % x

        self.server.applySchema(work.tableName, x)
        work.cb.done()

#----------------------------------------------------------------------------
# ConfigLoaderWork
#----------------------------------------------------------------------------
class ConfigLoaderWork:
    def __init__(self, cb, tabletName):
        self.cb = cb
        self.tabletName = tabletName

#----------------------------------------------------------------------------
# ConfigLoader
#----------------------------------------------------------------------------
class ConfigLoader(QueuedWorker):
    def __init__(self, server):
        super(ConfigLoader, self).__init__()
        self.server = server
        
    def doWork(self, work):
        x = Config()
        x.tableName,x.lastRow = decodeTabletName(work.tabletName)

        print 'Config loaded: %s' % x

        self.server.applyConfig(work.tabletName, x)
        work.cb.done()

#----------------------------------------------------------------------------
# MultipleErrors
#----------------------------------------------------------------------------
class MultipleErrors(Exception):
    def __init__(self, errors):
        self.errors = errors
    
    def __str__(self):
        return '\n'.join(str(x) for x in self.errors)

    def __repr__(self):
        return 'MultipleErrors(%r)' % self.errors

#----------------------------------------------------------------------------
# DelayedCbWrapper
#----------------------------------------------------------------------------
class DelayedCbWrapper:
    def __init__(self, cb):
        self.cb = cb
        self.ref = 1
        self.errors = []
        self._lock = threading.Lock()
    
    def addRef(self):
        self._lock.acquire()
        self.ref += 1
        self._lock.release()

    needSchema = addRef
    needConfig = addRef
    needLoad = addRef

    def error(self, err):
        self._lock.acquire()
        self.ref -= 1
        self.errors.append(err)
        if not self.ref:
            self._finish()
        self._lock.release()

    def done(self):
        self._lock.acquire()
        self.ref -= 1
        if not self.ref:
            self._finish()
        self._lock.release()

    def _finish(self):
        if not self.errors:
            self.cb.done()
        elif len(self.errors) == 1:
            self.cb.error(self.errors[0])
        else:
            self.cb.error(MultipleErrors(self.errors))


#----------------------------------------------------------------------------
# TabletServer
#----------------------------------------------------------------------------
class TabletServer:
    def __init__(self):
        self.tables = {}
        self.schemaLoader = SchemaLoader(self)
        self.configLoader = ConfigLoader(self)

    def start(self):
        self.schemaLoader.start()
        self.configLoader.start()

    def load_async(self, cb, tabletNames):
        delayedCb = DelayedCbWrapper(cb)
        for tabletName in tabletNames:
            tableName, lastRow = decodeTabletName(tabletName)

            if tableName not in self.tables:
                self.tables[tableName] = Table()
                delayedCb.needSchema()
                self.loadSchema_async(delayedCb, tableName)

            table = self.tables[tableName]

            if lastRow not in table.tablets:
                table.tablets[lastRow] = Tablet()
                delayedCb.needConfig()
                self.loadConfig_async(delayedCb, tabletName)

            tablet = table.tablets[lastRow]

            if tablet.getState() < TABLET_ACTIVE:
                delayedCb.needLoad()
                tablet.deferUntilLoaded(delayedCb)
            elif tablet.getState() > TABLET_ACTIVE:
                delayedCb.error(
                    RuntimeError('tablet is unloading: %s' % tabletName))
                return

        delayedCb.done()

    def loadSchema_async(self, cb, tableName):
        work = SchemaLoaderWork(cb, tableName)
        self.schemaLoader.submit(work)

    def loadConfig_async(self, cb, tabletName):
        work = ConfigLoaderWork(cb, tabletName)
        self.configLoader.submit(work)

    def applySchema(self, tableName, schema):
        self.tables[tableName].applySchema(schema)

    def applyConfig(self, tabletName, config):
        tableName, lastRow = decodeTabletName(tabletName)
        self.tables[tableName].tablets[lastRow].applyConfig(config)


#----------------------------------------------------------------------------
# TestCb
#----------------------------------------------------------------------------
class TestCb:
    def __init__(self):
        self._done = False
        self._err = None
        self._cond = threading.Condition()

    def done(self):
        print 'Done'
        self._cond.acquire()
        self._done = True
        self._cond.notifyAll()
        self._cond.release()

    def error(self, err):
        print 'Error: %r' % err
        self._cond.acquire()
        self._done = True
        self._err = err
        self._cond.notifyAll()
        self._cond.release()

    def wait(self):
        self._cond.acquire()
        while not self._done:
            self._cond.wait()
        self._cond.release()

#----------------------------------------------------------------------------
# main
#----------------------------------------------------------------------------
def main():
    server = TabletServer()
    server.start()
    cb = TestCb()

    server.load_async(cb, ['foo!', 'bar!', 'foo banjo'])
    cb.wait()

if __name__ == '__main__':
    main()
