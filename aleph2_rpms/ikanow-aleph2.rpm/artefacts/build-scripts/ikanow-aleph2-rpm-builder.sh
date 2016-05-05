#!/bin/sh

# Call with VERSION RELEASETYPE

if [ "$#" -ne 2 ]; then
    echo "args: version(eg 2.x.<date +%y%j%H>) releasetype(nightly|debug)"
    exit -1
fi

export VERSION=$1
export RELEASETYPE=$2

# Just c/p Rob's code for now:

mkdir dist
find Aleph2 -name "*-shaded.jar" -exec cp '{}' dist/ \;
find Aleph2-contrib -name "*-shaded.jar" -exec cp '{}' dist/ \;
echo "Building archive: aleph2_alljars-${VERSION}-${RELEASETYPE}-shaded.tgz"
tar czvf aleph2_alljars-${VERSION}-${RELEASETYPE}-shaded.tgz dist/

# Create Source Tarball and Structure for RPM
cp -rv Aleph2-contrib/aleph2_rpms/ikanow-aleph2.rpm/SOURCES/* dist/
rm -rf ikanow-aleph2-${VERSION}-${RELEASETYPE}
mv dist/ ikanow-aleph2-${VERSION}-${RELEASETYPE}
mkdir -p ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/lib
mkdir -p ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/server/lib
mv ikanow-aleph2-${VERSION}-${RELEASETYPE}/*.jar ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/lib
#SPECIAL CASE: move this one file into server/lib (so tomcat doesn't have access to it)
mv ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/lib/aleph2_server_only_dependencies* ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/server/lib 
mkdir -p ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/etc
mkdir -p ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/etc/conf.d
mkdir -p ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/etc/sysconfig/
mkdir -p ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/logs
mkdir -p ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/yarn-config
mkdir -p ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/run
mkdir -p ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/cached-jars
mkdir -p ikanow-aleph2-${VERSION}-${RELEASETYPE}/var/run/ikanow
mv ikanow-aleph2-${VERSION}-${RELEASETYPE}/etc/v1_sync_service.properties ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/etc
mv ikanow-aleph2-${VERSION}-${RELEASETYPE}/etc/sysconfig/ikanow-aleph2 ikanow-aleph2-${VERSION}-${RELEASETYPE}/etc/sysconfig/
mv ikanow-aleph2-${VERSION}-${RELEASETYPE}/etc/log4j2.xml ikanow-aleph2-${VERSION}-${RELEASETYPE}/opt/aleph2-home/etc
tar czvf ikanow-aleph2-${VERSION}-${RELEASETYPE}.tar.gz ikanow-aleph2-${VERSION}-${RELEASETYPE}

# Build the RPM
mkdir rpmbuild
cd rpmbuild
mkdir BUILD  BUILDROOT  RPMS  SOURCES  SPECS  SRPMS
export TOPDIR=$(pwd)
cd ..
cp ikanow-aleph2-${VERSION}-${RELEASETYPE}.tar.gz rpmbuild/SOURCES/
cp Aleph2-contrib/aleph2_rpms/ikanow-aleph2.rpm/SPECS/aleph2.spec rpmbuild/SPECS/
cd rpmbuild/SPECS/
rpmbuild --define "_topdir $TOPDIR" --define "_VERSION ${VERSION}" --define "_RELEASE ${RELEASETYPE}" -ba aleph2.spec
