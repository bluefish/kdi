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
import random
import heapq

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

TABLET_ERROR = 1000

#----------------------------------------------------------------------------
# name util
#----------------------------------------------------------------------------
def decodeTabletName(x):
    n = len(x) + 1
    i = min(x.find(' ') % n,
            x.find('!') % n)
    return x[:i], x[i:]

#----------------------------------------------------------------------------
# util
#----------------------------------------------------------------------------
def randomString(minLen, maxLen):
    return ''.join(chr(random.randint(ord('a'),ord('z')))
                   for i in xrange(random.randint(minLen, maxLen)))

def log(msg, _lock=threading.Lock()):
    _lock.acquire()
    print msg
    _lock.release()

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

def defaultSchema(tableName):
    x = Schema()
    x.tableName = tableName
    x.groups.append(Schema.Group())
    return x

def randomSchema(tableName):
    x = defaultSchema(tableName)
    used = set()
    for i in xrange(random.randrange(1,5)):
        g = Schema.Group()
        for j in xrange(random.randrange(1,4)):
            while True:
                f = randomString(3,10)
                if f not in used:
                    break
            g.families.append(f)
            used.add(f)
        x.groups.append(g)
    return x

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

    def getTabletName(self):
        return self.tableName + self.lastRow

    def empty(self):
        return not (self.fragments or self.log or self.location)

    def __str__(self):
        return 'Config(%s%s)' % (self.tableName, self.lastRow)

def randomConfig(tabletName):
    x = Config()
    x.tableName, x.lastRow = decodeTabletName(tabletName)
    for i in xrange(random.randrange(1,5)):
        f = Config.Fragment()
        f.filename = randomString(5,15)
        fams = set()
        for j in xrange(random.randrange(1,4)):
            fams.add(randomString(3,10))
        f.families = list(fams)
        x.fragments.append(f)
    return x

#----------------------------------------------------------------------------
# Fragment
#----------------------------------------------------------------------------
class Fragment:
    def __init__(self, filename, families):
        self.filename = filename
        self.families = families
    
    def getFileName(self):
        return self.filename

    def getFamilies(self):
        return self.families

#----------------------------------------------------------------------------
# Tablet
#----------------------------------------------------------------------------
class Tablet:
    def __init__(self, schema, lastRow):
        self.state = TABLET_CONFIG_LOADING
        self._stateq = []
        self.fragments = []
        self.loadingFrags = None
        self.schema = schema
        self.lastRow = lastRow

    def getState(self):
        return self.state

    def deferUntilState(self, cb, state):
        heapq.heappush(self._stateq, (state, cb))

    def setState(self, state):
        self.state = state
        callbacks = []
        while self._stateq:
            state,cb = self._stateq[0]
            if state > self.state:
                break
            heapq.heappop(self._stateq)
            callbacks.append(cb)
        if callbacks:
            def issueCbs():
                for cb in callbacks:
                    cb.done()
            return issueCbs
        else:
            return None

    def setLoadError(self, err):
        self.state = TABLET_ERROR
        callbacks = [cb for state,cb in self._stateq]
        self._stateq = []
        if callbacks:
            def issueCbs():
                for cb in callbacks:
                    cb.error(err)
            return issueCbs
        else:
            return None

    def getTableName(self):
        return self.schema.tableName
    
    def getLastRow(self):
        return self.lastRow

    def getFragmentConfigs(self):
        r = []
        if self.loadingFrags:
            r.extend(self.loadingFrags)
        for frag in self.fragments:
            cf = Config.Fragment()
            cf.filename = frag.getFilename()
            cf.families = frag.getFamilies()
            r.append(cf)
        return r

    def makePlaceholderFragments(self, fragments):
        log('Placeholder: %d frags' % len(fragments))
        self.loadingFrags = fragments

    def applyLoadedFragments(self, fragments):
        log('Apply fragments: %d frags' % len(fragments))
        self.fragments[:0] = fragments
        self.loadingFrags = None

    def applySchema(self, schema):
        self.schema = schema

#----------------------------------------------------------------------------
# Table
#----------------------------------------------------------------------------
class Table:
    def __init__(self, tableName):
        self.state = TABLE_SCHEMA_LOADING
        self.tablets = {}
        self.schema = defaultSchema(tableName)

    def applySchema(self, schema):
        log('Schema applied: %s' % schema)
        self.state = TABLE_ACTIVE
        self.schema = schema
        for tablet in self.tablets.itervalues():
            tablet.applySchema(schema)

    def getState(self):
        return self.state

#----------------------------------------------------------------------------
# QueuedWorker
#----------------------------------------------------------------------------
class QueuedWorker(threading.Thread):
    def __init__(self, name=None):
        super(QueuedWorker, self).__init__()
        self._name = name or self.__class__.__name__
        self._q = Queue.Queue()
        self.daemon = True

    def run(self):
        log('%s worker starting' % self._name)
        try:
            while True:
                work = self._q.get()
                work.doWork()
                self._q.task_done()
        except Exception, err:
            log('%s worker error: %s' % (self._name, err))
        log('%s worker exiting' % self._name)

    def submit(self, work):
        self._q.put(work)

#----------------------------------------------------------------------------
# SchemaLoaderWork
#----------------------------------------------------------------------------
class SchemaLoaderWork:
    def __init__(self, cb, tableName):
        self.cb = cb
        self.tableName = tableName

    def doWork(self):
        log('Loading schema: %s' % self.tableName)
        x = randomSchema(self.tableName)
        self.cb.done(x)

#----------------------------------------------------------------------------
# ConfigLoaderWork
#----------------------------------------------------------------------------
class ConfigLoaderWork:
    def __init__(self, cb, tabletName):
        self.cb = cb
        self.tabletName = tabletName

    def doWork(self):
        log('Loading config: %s' % self.tabletName)
        x = randomConfig(self.tabletName)
        self.cb.done(x)

#----------------------------------------------------------------------------
# ConfigSaverWork
#----------------------------------------------------------------------------
class ConfigSaverWork:
    def __init__(self, cb, cfg):
        self.cb = cb
        self.cfg = cfg

    def doWork(self):
        log('Saving config: %s' % self.cfg)
        self.cb.done()

#----------------------------------------------------------------------------
# FragmentLoaderWork
#----------------------------------------------------------------------------
class FragmentLoaderWork:
    def __init__(self, cb, fragments):
        self.cb = cb
        self.fragments = fragments

    def doWork(self):
        log('Loading %d fragment(s)' % len(self.fragments))
        frags = []
        for cf in self.fragments:
            log('Loading fragment: %s' % cf.filename)
            if 'b' in cf.filename:
                self.cb.error(
                    RuntimeError("we don't like B's: %s" % cf.filename))
                return
            f = Fragment(cf.filename, cf.families)
            frags.append(f)
        self.cb.done(frags)

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
# FragmentsLoadedCb
#----------------------------------------------------------------------------
class FragmentsLoadedCb:
    def __init__(self, server, tabletName):
        self.server = server
        self.tabletName = tabletName

    def done(self, fragments):
        log('Fragments loaded: %s' % self.tabletName)

        tablet = self.server.getTablet(self.tabletName)
        assert tablet.getState() == TABLET_FRAGMENTS_LOADING

        tablet.applyLoadedFragments(fragments)
        cb = tablet.setState(TABLET_ACTIVE)
        if cb:
            cb()
    
    def error(self, err):
        log('Fragment load error: %s' % err)
        self.server.tabletLoadError(self.tabletName, err)
        

#----------------------------------------------------------------------------
# ConfigSavedCb
#----------------------------------------------------------------------------
class ConfigSavedCb:
    def __init__(self, server, config):
        self.server = server
        self.config = config

    def done(self):
        log('Config saved: %s' % self.config)
        
        tablet = self.server.getTablet(self.config.getTabletName())
        assert tablet.getState() == TABLET_CONFIG_SAVING
        
        if self.config.fragments:
            cb = tablet.setState(TABLET_FRAGMENTS_LOADING)
            self.server.loadFragments_async(
                FragmentsLoadedCb(self.server, self.config.getTabletName()),
                self.config.fragments)
        else:
            cb = tablet.setState(TABLET_ACTIVE)
        if cb:
            cb()

    def error(self, err):
        log('Config save error: %s' % err)
        self.server.tabletLoadError(self.config.getTabletName(), err)


#----------------------------------------------------------------------------
# ReplayDoneCb
#----------------------------------------------------------------------------
class ReplayDoneCb:
    def __init__(self, server, config):
        self.server = server
        self.config = config

    def done(self):
        log('Log replayed: %s' % self.config)
        
        tablet = self.server.getTablet(self.config.getTabletName())
        assert tablet.getState() == TABLET_LOG_REPLAYING
        
        cb = tablet.setState(TABLET_CONFIG_SAVING)
        self.server.saveConfig_async(ConfigSavedCb(self.server, self.config), tablet)
        if cb:
            cb()

    def error(self, err):
        log('Log replay error: %s' % err)
        self.server.tabletLoadError(self.config.getTabletName(), err)


#----------------------------------------------------------------------------
# ConfigLoadedCb
#----------------------------------------------------------------------------
class ConfigLoadedCb:
    def __init__(self, server, tabletName):
        self.server = server
        self.tabletName = tabletName

    def done(self, config):
        log('Config loaded: %s' % config)

        tablet = self.server.getTablet(self.tabletName)
        assert tablet.getState() == TABLET_CONFIG_LOADING
        assert self.tabletName == config.getTabletName()
        
        tablet.makePlaceholderFragments(config.fragments)

        if config.log:
            cb = tablet.setState(TABLET_LOG_REPLAYING)
            self.server.replayLogs_async(ReplayDoneCb(self.server, config), config.log)
        else:
            cb = tablet.setState(TABLET_CONFIG_SAVING)
            self.server.saveConfig_async(ConfigSavedCb(self.server, config), tablet)
        if cb:
            cb()

    def error(self, err):
        log('Config load error: %s' % err)
        self.server.tabletLoadError(self.tabletName, err)

#----------------------------------------------------------------------------
# SchemaLoadedCb
#----------------------------------------------------------------------------
class SchemaLoadedCb:
    def __init__(self, server, tableName):
        self.server = server
        self.tableName = tableName

    def done(self, schema):
        log('Schema loaded: %s' % schema)

        table = self.server.getTable(self.tableName)
        assert self.tableName == schema.tableName

        table.applySchema(schema)

    def error(self, err):
        log('Schema load error: %s', err)
        self.server.schemaLoadError(self.tableName, err)

#----------------------------------------------------------------------------
# TabletServer
#----------------------------------------------------------------------------
class TabletServer:
    def __init__(self, log, location):
        self.tables = {}
        self.configWorker   = QueuedWorker('Config')
        self.fragmentLoader = QueuedWorker('Fragment')
        self.log = log
        self.location = location

    def start(self):
        self.configWorker.start()
        self.fragmentLoader.start()

    def load_async(self, cb, tabletNames):
        delayedCb = DelayedCbWrapper(cb)
        for tabletName in tabletNames:
            tableName, lastRow = decodeTabletName(tabletName)

            if tableName not in self.tables:
                self.tables[tableName] = Table(tableName)
                self.loadSchema_async(SchemaLoadedCb(self, tableName), tableName)

            table = self.tables[tableName]

            if lastRow not in table.tablets:
                table.tablets[lastRow] = Tablet(tableName, lastRow)
                self.loadConfig_async(ConfigLoadedCb(self, tabletName), tabletName)

            tablet = table.tablets[lastRow]

            if tablet.getState() < TABLET_ACTIVE:
                delayedCb.needLoad()
                tablet.deferUntilState(delayedCb, TABLET_ACTIVE)
            elif tablet.getState() > TABLET_ACTIVE:
                delayedCb.error(
                    RuntimeError('tablet is unloading: %s' % tabletName))
                return

        delayedCb.done()

    def loadSchema_async(self, cb, tableName):
        work = SchemaLoaderWork(cb, tableName)
        self.configWorker.submit(work)

    def loadConfig_async(self, cb, tabletName):
        work = ConfigLoaderWork(cb, tabletName)
        self.configWorker.submit(work)

    def saveConfig_async(self, cb, tablet):
        cfg = Config()
        cfg.log = self.log
        cfg.location = self.location
        cfg.tableName = tablet.getTableName()
        cfg.lastRow = tablet.getLastRow()
        cfg.fragments = tablet.getFragmentConfigs()
        work = ConfigSaverWork(cb, cfg)
        self.configWorker.submit(work)

    def loadFragments_async(self, cb, fragments):
        work = FragmentLoaderWork(cb, fragments)
        self.fragmentLoader.submit(work)

    def getTable(self, tableName):
        return self.tables[tableName]

    def getTablet(self, tabletName):
        tableName, lastRow = decodeTabletName(tabletName)
        return self.tables[tableName].tablets[lastRow]

    def tabletLoadError(self, tabletName, err):
        tablet = self.getTablet(tabletName)
        cb = tablet.setLoadError(err)
        if cb:
            cb()


#----------------------------------------------------------------------------
# TestCb
#----------------------------------------------------------------------------
class TestCb:
    def __init__(self):
        self._done = False
        self._err = None
        self._cond = threading.Condition()

    def done(self):
        log('Done')
        self._cond.acquire()
        self._done = True
        self._cond.notifyAll()
        self._cond.release()

    def error(self, err):
        log('Error: %r' % err)
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
    server = TabletServer('log','location')
    server.start()
    cb = TestCb()

    server.load_async(cb, ['foo!', 'bar!', 'foo banjo'])
    cb.wait()

if __name__ == '__main__':
    main()
