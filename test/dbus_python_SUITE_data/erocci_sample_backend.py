#!/usr/bin/env python3
#
# Sample python backend - in memory storage
#
from gi.repository import GObject
import dbus
import dbus.glib

import os
import sys
import dbus
import dbus.service
import signal
import uuid

IFACE = "org.ow2.erocci.backend.core"
SERVICE = "org.ow2.erocci.backend.SampleService"

BASEDIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATH = os.path.abspath(os.path.join(BASEDIR, "occi-infrastructure.xml"))
SCHEMA_TYPE_XML = 0

LINK_SOURCE = 0
LINK_TARGET = 1


def log(msg):
    sys.stderr.write("[INFO] " + msg + "\n")
    return
    
    
def get_schema():
    content = ''
    with open(SCHEMA_PATH, 'r') as fh:
        for line in fh:
            content = content + line
        log("Load schema from %s" % (SCHEMA_PATH,))
    return content


class NotFound(dbus.exceptions.DBusException):
    _dbus_error_name = "org.ow2.erocci.backend.NotFound"


class Conflict(dbus.exceptions.DBusException):
    _dbus_error_name = "org.ow2.erocci.backend.Conflict"


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
            category = str(category)
            if not category in self.__collections:
                self.__collections[category] = set()
            self.__collections[category].add(location)
        return


    def __rm_collections(self, location, categories=False):
        if categories == False:
            categories = self.__collections.keys()
        for category in categories:
            if category in self.__collections:
                category = str(category)
                if location in self.__collections[category]:
                    self.__collections[category].remove(location)
        return


    def __rm_endpoint(self, endpoint):
        for link in self.__links.keys():
            if endpoint in self.__links[link]:
                self.__links[link].remove(endpoint)
        return


    def __get_links(self, location):
        links = []
        for (link, entries) in self.__links.items():
            for (direction, endpoint) in entries:
                if location == endpoint:
                    links.append(link)
                    break
        return links
        

    def __get(self, location):
        if location in self.__entities:
            (kind, mixins, attributes, owner, group, serial) = self.__entities[location]
            if 'occi.core.source' in attributes:
                return (kind, list(mixins), attributes, [], owner, group, serial)
            else:
                return (kind, list(mixins), attributes, self.__get_links(location), owner, group, serial)
        else:
            raise NotFound("%s entity does not exists" % (location))


    ##
    ## Interface: org.ow2.erocci.backend.core
    ##
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='a{sv}', out_signature='', byte_arrays=True)
    def Init(self, opts):
        log("Init(%s)" % (opts))
        return 

    
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='', out_signature='', byte_arrays=True)
    def Terminate(self):
        log("Terminate()")
        exit(1)

    
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='', out_signature='a(ys)', byte_arrays=True)
    def Models(self):
        log("Models()")
        return [ (SCHEMA_TYPE_XML, self.__schema)]
    

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='s', out_signature='sasa{sv}assss', byte_arrays=True)
    def Get(self, location):
        location = str(location)
        log("Get(%s)" % (location))
        return self.__get(location)

    
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='ssasa{sv}ss', out_signature='sasa{sv}ass', byte_arrays=True)
    def Create1(self, location, kind, mixins, attributes, owner, group):
        location = str(location)
        log("Create1(%s)" % (location))
        if location in self.__entities[location]:
            (e_kind, e_mixins, e_attributes, e_owner, e_group, e_serial) = self.__entities[location]
            if e_owner == owner:
                self.__entities[location] = (kind, set(mixins), attributes, owner, group, e_serial)
                self.__add_collections(location, [kind] + mixins)
                return (kind, mixins, attributes, self.__get_links(location), serial)
            else:
                raise Conflict("%s entity does not exists" % (location))
        else:
            serial = str()
            self.__entities[location] = (kind, set(mixins), attributes, owner, group, serial)
            self.__add_collections(location, [kind] + mixins)
            return (kind, mixins, attributes, [], serial)

    
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sasa{sv}ss', out_signature='ssasa{sv}ass', byte_arrays=True)
    def Create2(self, kind, mixins, attributes, owner, group):
        log("Create2(%s)" % (kind))
        location = ''
        if 'occi.core.id' in attributes:
            location = str(attributes['occi.core.id'])
        else:
            location = '%s' % uuid.uuid4()
        serial = str()
        self.__entities[location] = (kind, set(mixins), attributes, owner, group, serial)
        self.__add_collections(location, [kind] + mixins)
        return (location, kind, mixins, attributes, [], serial)

    
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sa{sv}', out_signature='sasa{sv}ass', byte_arrays=True)
    def Update(self, location, attributes):
        location = str(location)
        log("Update(%s)" % (location))
        if location in self.__entities:
            (kind, mixins, actual, owner, group, serial) = self.__entities[location]
            for (k, v) in attributes:
                actual[str(k)] = v
            self.__entities[location] = (kind, mixins, actual, owner, group, serial)
            return (kind, list(mixins), actual, self.__get_links(location), serial)
        else:
            raise NotFound('%s entity does not exists' % (location))


    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sys', out_signature='', byte_arrays=True)
    def Link(self, location, direction, link):
        location = str(location)
        link = str(link)
        log("Link(%s, %s)" % (location, link))
        if not link in self.__links:
            self.__links[link] = set()
        self.__links[link].add( (direction, location) )
        return


    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='ssa{sv}', out_signature='sasa{sv}ass', byte_arrays=True)
    def Action(self, location, action, attributes):
        location = str(location)
        log("Action(%s, %s)" % (location, action))
        if location in self.__entities:
            (kind, mixins, attributes, owner, group, serial) = self.__entities[location]
            return (kind, list(mixins), attributes, self.__get_links(location), serial)
        else:
            raise NotFound('%s entity does not exists' % (location))
        

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='s', out_signature='', byte_arrays=True)
    def Delete(self, location):
        location = str(location)
        log("Delete(%s)" % (location))
        if location in self.__entities:
            (kind, mixins, attributes, owner, group, serial) = self.__entities[location]
            if 'occi.core.source' in attributes:
                del self.__links[location]
            else:
                self.__rm_endpoint(location)
            categories = [kind] + list(mixins)
            self.__rm_collections(location, categories)
            del self.__entities[location]
        else:
            raise NotFound('%s entity does not exists' % (location))
        return


    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='ssa{sv}', out_signature='sasa{sv}ass', byte_arrays=True)
    def Mixin(self, location, mixin, attributes):
        location = str(location)
        mixin = str(mixin)
        log("Mixin(%s, %s)" % (location, mixin))
        if location in self.__entities:
            (kind, mixins, actual, owner, group, serial) = self.__entities[location]
            self.__add_collections(location, [mixin])
            for (k, v) in attributes:
                actual[str(k)] = v
            mixins.add(mixin)
            self.__entities[location] = (kind, mixins, actual, owner, group, serial)
            return (kind, list(mixins), actual, self.__get_links(location), serial)
        else:
            raise NotFound('%s entity does not exists' % (location))

    
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='ss', out_signature='sasa{sv}ass', byte_arrays=True)
    def Unmixin(self, location, mixin):
        location = str(location)
        mixin = str(mixin)
        log("Unmixin(%s, %s)" % (location, mixin))
        if location in self.__entities:
            (kind, mixins, actual, owner, group, serial) = self.__entities[location]
            self.__rm_collections(location, [mixin])
            mixins.remove(mixin)
            self.__entities[location] = (kind, mixins, actual, owner, group, serial)
            return (kind, list(mixins), actual, self.__get_links(location), serial)
        else:
            raise NotFound('%s entity does not exists' % (location))


    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sa(ysv)ui', out_signature='a(ssasa{sv}asss)s', byte_arrays=True)
    def Collection(self, category, filter, start, number):
        category = str(category)
        start = int(start)
        number = int(number)
        # Do not handle unbounded collection, nor filters (just for demo)
        log("Collection(%s)" % (category))
        if category in self.__collections:
            locations = []
            ret = []
            if number == -1:
                locations = list(self.__collections[category])[start:]
            else:
                locations = list(self.__collections[category])[(start-1):number]
            for location in locations:
                (kind, mixins, attributes, owner, group, serial) = self.__entities[location]
                ret.append( (location, kind, list(mixins), attributes, self.__get_links(location), owner, group) )
            serial = str()
            return (ret, serial)
        else:
            return ([], '')

        
if len(sys.argv) > 1:
    SCHEMA_PATH = sys.argv[1]

service = SampleService()
log("Connection: %s" % (service.connection.activate_name_owner(SERVICE),))

mainloop = GObject.MainLoop()
mainloop.run()

