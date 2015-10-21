all:

JAVA_CI = CreateInterface

java: java/org/ow2/erocci/backend.java

java/org/ow2/erocci/backend.java: erocci-dbus-nodoc.xml
	mkdir -p java
	cd java && $(JAVA_CI) --session --create-files ../$< 

erocci-dbus-nodoc.xml: erocci-dbus.xml dbus-cleanup.xsl
	xsltproc dbus-cleanup.xsl $< > $@

clean:
	-rm -rf java
	-rm -f erocci-dbus-nodoc.xml

.PHONY: all java
