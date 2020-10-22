PLUGIN=geocoding
VERSION=0.1.0

all:
	echo "install"

install:
	cdap cli load artifact ./target/$(PLUGIN)-$(VERSION).jar config-file ./target/$(PLUGIN)-$(VERSION).json

build:
	mvn package -DskipTests

run: build install
	@echo "Done"

stop:
	cdap sandbox stop

start:
	cdap sandbox start --enable-debug

start2:
	cdap sandbox start