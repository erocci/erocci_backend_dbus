#!/usr/bin/env python
#
# Sample python backend - in memory storage
#
from gi.repository import Gtk
from dbus.mainloop.glib import DBusGMainLoop

import os
import dbus
import dbus.service
import signal
import uuid

IFACE = "org.ow2.erocci.backend.core"
SERVICE = "org.ow2.erocci.backend.SampleService"

SCHEMA_TYPE_XML = 0

LINK_SOURCE = 0
LINK_TARGET = 1

def get_schema():
    dirname = os.path.dirname(os.path.abspath(__file__))
    path = os.path.abspath(os.path.join(dirname, "occi-infrastructure.xml"))
    content = ''
    with open(path, 'r') as fh:
        for line in fh:
            content = content + line
        print "Load schema from %s\n" % (path,)
    return content

class SampleService(dbus.service.Object):

    def __init__(self):
        self.__schema = get_schema()
        self.__entities = {}
        self.__collections = {}
        self.__links = {}
        bus_name = dbus.service.BusName(SERVICE, bus=dbus.SessionBus())
        dbus.service.Object.__init__(self, bus_name, '/')


    def __add_collections(self, location, categories):
        for category in categories:
            if not category in self.__collections:
                self.__collections[category] = set()
            self.__collections[category].add(location)
        return


    def __rm_collections(self, location, categories=[]):
        for category in categories or self.__collections.keys():
            if category in self.__collections:
                if location in self.__collections[category]:
                    del self.__collections[category][location]
        return


    def __rm_endpoint(self, endpoint):
        for link in self.__links.keys():
            if endpoint in self.__links[link]:
                self.__links[link].remove(endpoint)
        return
    
        
    ##
    ## Interface: org.ow2.erocci.backend.core
    ##
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='a{sv}', out_signature='')
    def Init(self, opts):
        print "[INFO] Init(%s)\n" % (opts)
        return 

    
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='', out_signature='')
    def Terminate(self):
        print "[INFO] Terminate()\n"
        return

    
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='', out_signature='a(ys)')
    def Models(self):
        print "[INFO] Models()\n"
        return [ (SCHEMA_TYPE_XML, self.__schema)]

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='s', out_signature='sasa{sv}sss')
    def Get(self, location):
        print "[INFO] Get(%s)\n" % (location)
        if location in self.__entities:
            (kind, mixins, attributes, owner, group) = self.__entities[location]
            if 'occi.core.source' in attributes:
                return (kind, mixins, attributes, [], owner, group, '')
            else:
                links = []
                for (link, endpoints) in self.__links.items():
                    if endpoint in endpoints:
                        links.append(link)
                return (kind, mixins, attributes, links, owner, group, '')
        else:
            raise dbus.exception.DBusException(
                'org.ow2.erocci.NotFound',
                '%s entity does not exists' % (location)
            )       


    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='ssasa{sv}ss', out_signature='sasa{sv}s')
    def Create1(self, location, kind, mixins, attributes, owner, group):
        print "[INFO] Create1(%s)\n" % (location)
        if location in self.__entities[location]:
            (e_kind, e_mixins, e_attributes, e_owner, e_group) = self.__entities[location]
            if e_owner == owner:
                self.__entities[location] = (kind, set(mixins), attributes, owner, group)
                self.__add_collections(location, [kind] + mixins)
                return (kind, mixins, attributes, '')
            else:
                raise dbus.exception.DBusException(
                    'org.ow2.erocci.Conflict',
                    '%s entity does not exists' % (location)
                )
        else:
            self.__entities[location] = (kind, set(mixins), attributes, owner, group)
            self.__add_collection(location, [kind] + mixins)
            return (kind, mixins, attributes, '')

    
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sasa{sv}ss', out_signature='ssasa{sv}s')
    def Create2(self, kind, mixins, attributes, owner, group):
        print "[INFO] Create2(%s)\n" % (kind)
        location = ''
        if 'occi.core.id' in attributes:
            location = attributes['occi.core.id']
        else:
            location = '%s' % uuid.uuid4()
        self.__entities[location] = (kind, set(mixins), attributes, owner, group)
        self.__add_collections(location, [kind] + mixins)
        return (location, kind, mixins, attributes, '')

    
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sa{sv}', out_signature='sasa{sv}s')
    def Update(self, location, attributes):
        print "[INFO] Update(%s)\n" % (location)
        if location in self.__entities:
            (kind, mixins, actual, owner, group) = self.__entities[location]
            for (k, v) in attributes:
                actual[k] = v
            self.__entities[location] = (kind, mixins, actual, owner, group)
            return (kind, mixins, actual, '')
        else:
            raise dbus.exception.DBusException(
                'org.ow2.erocci.NotFound',
                '%s entity does not exists' % (location)
            )       


    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sys', out_signature='')
    def Link(self, location, direction, link):
        print "[INFO] Link(%s, %s, %s)\n" % (location, direction, link)
        if not link in self.__links:
            self.__links[link] = set()
        self.__links[link].add( (direction, location) )
        return


    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='ssa{sv}', out_signature='sasa{sv}s')
    def Action(self, location, action, attributes):
        print "[INFO] Action(%s, %s)\n" % (location, action)
        if location in self.__entities:
            (kind, mixins, attributes, owner, group) = self.__entities[location]
            return (kind, mixins, attributes, '')
        else:
            raise dbus.exception.DBusException(
                'org.ow2.erocci.NotFound',
                '%s entity does not exists' % (location)
            )
        

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='s', out_signature='')
    def Delete(self, location):
        print "[INFO] Delete(%s)\n" % (location)
        if location in self.__entities:
            (kind, mixins, attributes, owner, group) = self.__entities[location]
            if 'occi.core.source' in attributes:
                del self.__links[location]
            else:
                self.__rm_endpoint(location)
            self.__rm_collections(location, [kind] + mixins)
            del d[location]
        else:
            raise dbus.exception.DBusException(
                'org.ow2.erocci.NotFound',
                '%s entity does not exists' % (location)
            )
        return


    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='ssa{sv}', out_signature='sasa{sv}s')
    def Mixin(self, location, mixin, attributes):
        if location in self.__entities:
            (kind, mixins, actual, owner, group) = self.__entities[location]
            self.__add_collections(location, [mixin])
            for (k, v) in attributes:
                actual[k] = v
            mixins.add(mixin)
            self.__entities[location] = (kind, mixins, actual, owner, group)
            return (kind, mixins, actual, '')
        else:
            raise dbus.exception.DBusException(
                'org.ow2.erocci.NotFound',
                '%s entity does not exists' % (location)
            )

    
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='ss', out_signature='sasa{sv}s')
    def Unmixin(self, location, mixin):
        if location in self.__entities:
            (kind, mixins, actual, owner, group) = self.__entities[location]
            self.__rm_collections(location, [mixin])
            mixins.remove(mixin)
            self.__entities[location] = (kind, mixins, actual, owner, group)
            return (kind, mixins, actual, '')
        else:
            raise dbus.exception.DBusException(
                'org.ow2.erocci.NotFound',
                '%s entity does not exists' % (location)
            )


    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sa(ysv)uu', out_signature='a(sasa{sv}sss)')
    def Collection(self, category, filter, start, number):
        # Do not handle unbounded collection, nor filters (just for demo)
        if category in self.__collections:
            if number == -1:
                return list(self.__collections)[start:]
            else:
                return list(self.__collections)[(start-1):number]
        else:
            raise dbus.exception.DBusException(
                'org.ow2.erocci.NotFound',
                '%s entity does not exists' % (location)
            )    


signal.signal(signal.SIGINT, signal.SIG_DFL)
DBusGMainLoop(set_as_default=True)
service = SampleService()
print("%s" % (service.connection.activate_name_owner(SERVICE),))
Gtk.main()

