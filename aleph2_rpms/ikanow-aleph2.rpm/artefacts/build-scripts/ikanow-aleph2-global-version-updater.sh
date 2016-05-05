#!/bin/sh
echo "Setting all version numbers to $1"
for i in $(find Aleph2* ikos-core* -name "*pom.xml" | grep -P -v 'test_dependencies|core_dependencies' ); 
	do echo $i; 
	sed -i "s|<aleph2.version>[0-9a-zA-Z._-]*</aleph2.version>|<aleph2.version>$1</aleph2.version>|g" $i; 
done
