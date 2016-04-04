erocci_backend_dbus
===================

D-Bus interface for erocci backends

# Backend skeleton

D-Bus interface for backends is completly described with an XML
backends in `priv/erocci-dbus.xml`. A backend skeleton can be
generated with the script `priv/generate.es`.

Usage:
```
priv/generate.es -o LANG Name
```

* LANG: output language. Available: python
* Name: Name of the backend.

# TODO

* Define return values for long requests: something like HTTP 202 Code
