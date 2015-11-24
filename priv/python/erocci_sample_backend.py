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

IFACE = "org.ow2.erocci.backend.core"
SERVICE = "org.ow2.erocci.backend.SampleService"

# Entity tuple indices
E_PARENT = 0
E_KIND = 1
E_MIXINS = 2
E_ATTRS = 3
E_OWNER = 4
E_SERIAL = 5

# Entity type instance
RESOURCE = 0
LINK = 1

# Core attributes
A_SOURCE = "occi.core.source"
A_TARGET = "occi.core.target"
A_LINKS = "occi.core.links"

# Node types (defined in D-Bus API)
N_ENTITY = 0        # entity
N_UNBOUNDED = 1     # unbounded collection

# Internal collection type
C_KIND = 0
C_MIXIN = 1
C_UNBOUNDED = 2

def get_schema():
    dirname = os.path.dirname(os.path.abspath(__file__))
    path = os.path.abspath(os.path.join(dirname, "occi.xml"))
    content = ''
    with open(path, 'r') as fh:
        for line in fh:
            content = content + line
        print "Load schema from %s\n" % (path,)
    return content

class SampleService(dbus.service.Object):

    def __init__(self):
        self.__schema = get_schema()
        self.__kinds = {}
        self.__mixins = {}
        self.__entities = {}
        bus_name = dbus.service.BusName(SERVICE, bus=dbus.SessionBus())
        dbus.service.Object.__init__(self, bus_name, '/')

    def __add_to_categories(self, _id, kind, mixins):
        print "add_to_categories(%s, %s, %s)" % (_id, kind, mixins)
        if not kind in self.__kinds:
            self.__kinds[kind] = set()
        self.__kinds[kind].add(_id)
        for mixin in mixins:
            if not mixin in self.__mixins:
                self.__mixins[mixin] = set()
            self.__mixins[mixin].add(_id)
        print self.__kinds

    def __del_entity(self, id):
        e = self.__entities[id]
        self.__kinds[e[E_KIND]].remove(id)
        for m in e[E_MIXINS]:
            self.__mixins[m].remove(id)
        del self.__entities[id]

    ##
    ## Interface: org.ow2.erocci.backend.core
    ##
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='a{sv}', out_signature='')
    def Init(self, opts):
        return 

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='', out_signature='')
    def Terminate(self):
        return 

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='ssasa{sv}s', out_signature='s')
    def SaveResource(self, resid, kind, mixins, attributes, owner):
        serial = 1
        attributes["occi.core.links"] = dbus.Array([], signature='s')
        self.__entities[resid] = (RESOURCE, kind, mixins, attributes, owner, serial)
        self.__add_to_categories(resid, kind, mixins)
        return resid

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='ssasssa{sv}s', out_signature='s')
    def SaveLink(self, linkid, kind, mixins, src, target, attributes, owner):
        serial = 1
        attributes = attributes[A_SOURCE] = src
        attributes = attributes[A_TARGET] = target
        self.__entities[linkid] = (LINK, kind, mixins, attributes, owner, serial)
        self.__add_to_categories(linkid, kind, mixins)
        self.__entities[src][E_ATTRS]['links'].append(linkid)
        self.__entities[target][E_ATTRS]['links'].append(linkid)
        return linkid

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sa{sv}', out_signature='a{sv}')
    def Update(self, entityid, attributes):
        if entityid in self.__entities:
            (parent, kind, mixins, attributes2, owner, serial) = self.__entities[entityid]
            for key in attributes:
                attributes2[key] = attributes[key]
            self.__entities[entityid] = (parent, kind, mixins, attributes2, owner, serial+1)
        else:
            raise dbus.exception.DBusException(
                'org.ow2.erocci.UnknownEntity',
                '%s entity does not exists' % (entityid)
            )
        return attributes2

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sas', out_signature='')
    def SaveMixin(self, mixinid, entities):
        self.__mixins[mixinid] = set(entities)
        for entity in entities:
            (parent, kind, mixins, attributes, owner, serial) = self.__entities[mixinid]
            mixins2 = mixins.append(mixinid)
            self.__entities[mixinid] = (parent, kind, mixins2, attributes, owner, serial+1)
        return

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sas', out_signature='')
    def UpdateMixin(self, mixinid, entities):
        if not mixinid in self.__mixins:
            self.__mixins[mixinid] = set()
        self.__mixins[mixinid] = self.__mixins[mixinid].intersection(entities)
        for entity in entities:
            (parent, kind, mixins, attributes, owner, serial) = self.__entities[mixinid]
            mixins2 = mixins.append(mixinid)
            self.__entities[mixinid] = (parent, kind, mixins2, attributes, owner, serial+1)
        return

    #
    # Find is called to get metadata of a node (entity, collection)
    #
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='s', out_signature='a(yvsu)')
    def Find(self, _id):
        if _id in self.__entities:
            (_parent, _kind, _mixins, _attributes, owner, serial) = self.__entities[_id]
            return [(N_ENTITY, _id, owner, serial)]
        else:
            col = [ entity_id for entity_id in self.__entities if entity_id.startswith(_id) ]
            if col == []:
                return []
            else:
                # Return unbounded collection without collection, will be possibly empty
                return [(N_UNBOUNDED, _id, "", 0)]

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='v', out_signature='ssasa{sv}')
    def Load(self, _id):
        if _id in self.__entities:
            (_parent, kind, mixins, attributes, _owner, _serial) = self.__entities[_id]
            return (_id, kind, mixins, attributes)
        else:
            raise dbus.exception.DBusException(
                'org.ow2.erocci.UnknownEntity',
                '%s entity does not exists' % (_id)
            )

    #
    # List / Next are called respectively to get metadata and content of a
    # collection
    #
    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='sa{sv}', out_signature='vu')
    def List(self, _id, _filters):
        if _id in self.__kinds:
            return ((C_KIND, _id), 0)
        elif _id in self.__mixins:
            return ((C_MIXIN, _id), 0)
        else:
            return ((C_UNBOUNDED, _id), 0)
        

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='vuu', out_signature='a(ss)')
    def Next(self, iter, start, items):
        (type, colid) = iter
        full_ids = []
        if type == C_KIND:
            full_ids = self.__kinds[colid]
        elif type == C_MIXIN:
            full_ids = self.__mixins[colid]
        else:
            full_ids = [ entity_id for entity_id in self.__entities if entity_id.startswith(colid) ]

        if items == 0:
            return [ (id, self.__entities[id][E_OWNER]) for id in full_ids ]
        else:
            return [ (id, self.__entities[id][E_OWNER]) for id in full_ids ]
        

    @dbus.service.method("org.ow2.erocci.backend.core", in_signature='s', out_signature='')
    def Delete(self, id):
        if id in self.__kinds:
            for i in self.__kinds[id]:
                entity = self.__entities[i]
                for m in entity[E_MIXINS]:
                    self.__mixins[m].remove(i)
                del self.__entities[i]
            del self.__kinds[id]
        elif id in self.__mixins:
            for e in self.__mixins[id]:
                e[E_MIXINS].remove(id)
            self.__mixins[id] = set()
        elif id in self.__entities:
            self.__del_entity(id)
        else:
            [ self.__del_entity(entity_id)
              for entity_id in self.__entities
              if entity_id.startswith(id) ]
        return 

    @dbus.service.method(dbus.PROPERTIES_IFACE, in_signature='ss', out_signature='v')
    def Get(self, interface_name, property_name):
        if interface_name == IFACE:
            if property_name == 'schema':
                return get_schema()
            else:
                raise dbus.exception.DBusException(
                    'org.ow2.erocci.UnknownProperty',
                    'The %s interface does not have %s property' % (IFACE, property_name))
        else:
            raise dbus.exception.DBusException(
                'org.ow2.erocci.UnknownInterface',
                'The / object does not implement the %s interface' % interface_name)
        
    @dbus.service.method(dbus.PROPERTIES_IFACE, in_signature='s', out_signature='a{sv}')
    def GetAll(self, interface_name):
        if interface_name == IFACE:
            return {'schema': self.__schema}
        else:
            raise dbus.exception.DBusException(
                'org.ow2.erocci.UnknownInterface',
                'The / object does not implement the %s interface' % interface_name)

    @dbus.service.signal(dbus.PROPERTIES_IFACE, signature='sa{sv}as')
    def PropertiesChanged(self, interface_name, changed_properties, invalidated_properties):
        pass 


signal.signal(signal.SIGINT, signal.SIG_DFL)
DBusGMainLoop(set_as_default=True)
service = SampleService()
print("%s" % (service.connection.activate_name_owner(SERVICE),))
Gtk.main()

