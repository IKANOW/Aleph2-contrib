#!/bin/sh

# Call with VERSION RELEASETYPE

if [ "$#" -ne 2 ]; then
    echo "args: version(eg 2.x.<date +%y%j%H>) releasetype(nightly|debug)"
    exit -1
fi

export VERSION=$1
export RELEASETYPE=$2

export HOME_DIR=./
export RPM_NAME=ikanow-aleph2-apps-$VERSION-$RELEASETYPE

export RPM_SOURCE_DIR=$HOME_DIR/Aleph2-contrib/aleph2_rpms/ikanow-aleph2-apps.rpm/
export RPM_BUILD_DIR=$HOME_DIR/$RPM_NAME

###############################################
#
# Prepare files for building

rm -rf $RPM_BUILD_DIR
mkdir -p $RPM_BUILD_DIR
cp -rv $RPM_SOURCE_DIR/SOURCES/* $RPM_BUILD_DIR/

###############################################
#
# Copy plugin binaries and assets across

export A2BB_TEMPLATES=$RPM_BUILD_DIR/opt/aleph2-home/apps/templates/aleph2_bucket_builder/
export PLUGINS_DIR=$RPM_BUILD_DIR/opt/aleph2-home/apps/plugins

cp $HOME_DIR/Aleph2-examples/aleph2_enrichment_utils/target/*-shaded.jar $PLUGINS_DIR
cp $HOME_DIR/Aleph2-examples/aleph2_enrichment_utils/assets/batch_analytics_templates.json $A2BB_TEMPLATES/

cp $HOME_DIR/Aleph2-examples/aleph2_flume_harvester/target/*-shaded.jar $PLUGINS_DIR

cp $HOME_DIR/Aleph2-examples/aleph2_logstash_harvester/target/*-shaded.jar $PLUGINS_DIR
cp $HOME_DIR/Aleph2-examples/aleph2_logstash_harvester/assets/logstash_forms.json $A2BB_TEMPLATES/

cp $HOME_DIR/Aleph2-examples/aleph2_script_harvester/target/*-shaded.jar $PLUGINS_DIR

cp $HOME_DIR/Aleph2-examples/aleph2_storm_script_topo/target/*-shaded.jar $PLUGINS_DIR

cp $HOME_DIR/Aleph2-contrib/aleph2_analytic_services_spark/target/*-shaded.jar $PLUGINS_DIR/spark_technology.jar
cp $HOME_DIR/Aleph2-contrib/aleph2_analytic_services_spark/assets/spark_forms.json $A2BB_TEMPLATES/

###############################################
#
# Build the RPM

tar --exclude='.gitignore' -czvf ${RPM_NAME}.tar.gz $RPM_BUILD_DIR/

mkdir -p rpmbuild
cd rpmbuild
mkdir -p BUILD  BUILDROOT  RPMS  SOURCES  SPECS  SRPMS
export TOPDIR=$(pwd)
cd ..
cp ${RPM_NAME}.tar.gz rpmbuild/SOURCES/
cp $RPM_SOURCE_DIR/SPECS/aleph2-apps.spec rpmbuild/SPECS/
cd rpmbuild/SPECS/
rpmbuild --define "_topdir $TOPDIR" --define "_VERSION ${VERSION}" --define "_RELEASE ${RELEASETYPE}" -ba aleph2-apps.spec

