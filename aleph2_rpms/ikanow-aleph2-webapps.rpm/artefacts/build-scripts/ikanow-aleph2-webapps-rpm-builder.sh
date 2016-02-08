#!/bin/sh

# Call with VERSION RELEASETYPE

if [ "$#" -ne 2 ]; then
    echo "args: version(eg 2.x.<date +%y%j%H>) releasetype(nightly|debug)"
    exit -1
fi

export VERSION=$1
export RELEASETYPE=$2

export HOME_DIR=./
export RPM_NAME=ikanow-aleph2-webapps-$VERSION-$RELEASETYPE

export RPM_SOURCE_DIR=$HOME_DIR/Aleph2-contrib/aleph2_rpms/ikanow-aleph2-webapps.rpm/
export RPM_BUILD_DIR=$HOME_DIR/$RPM_NAME

export WEBAPPS_DIR=$RPM_BUILD_DIR/opt/tomcat-infinite/interface-engine/webapps/

###############################################
#
# Prepare files for building

rm -rf $RPM_BUILD_DIR
mkdir -p $RPM_BUILD_DIR
cp -rv $RPM_SOURCE_DIR/SOURCES/* $RPM_BUILD_DIR/

###############################################
#
# Copy bucket builder files across

cp $HOME_DIR/aleph2_bucket_builder/stable_war/aleph2_bucket_builder.war $WEBAPPS_DIR

export A2BB_TEMPLATES=$RPM_BUILD_DIR/opt/aleph2-home/webapps/templates/aleph2_bucket_builder/

cp $HOME_DIR/aleph2_bucket_builder/assets/json/generic_bucket_templates.json $A2BB_TEMPLATES/
cp $HOME_DIR/Aleph2-examples/aleph2_enrichment_utils/assets/batch_analytics_templates.json $A2BB_TEMPLATES/

###############################################
#
# Copy SSO files across

export WEBAPPS_LIB_DIR=$RPM_BUILD_DIR/opt/aleph2-home/webapps/lib/

export WEB_SSO_HOME=$HOME_DIR/Aleph2-examples/aleph2_web_sso/
export WEB_UTILS_HOME=$HOME_DIR/Aleph2-examples/aleph2_web_utils/

cp $WEB_SSO_HOME/target/aleph2_web_sso.war $WEBAPPS_DIR
cp $WEB_UTILS_HOME/target/aleph2_web_utils-*-shaded.jar $WEBAPPS_LIB_DIR

export SSO_CONFIG=$RPM_BUILD_DIR/opt/aleph2-home/etc/aleph2_web_sso/
export SSO_CONFIG_TEMPLATES=$WEB_SSO_HOME/src/main/resources/

#classpath:
cp $SSO_CONFIG_TEMPLATES/idp-metadata.xml_template $SSO_CONFIG/idp-metadata.xml
cp $SSO_CONFIG_TEMPLATES/samlKeystore.jks_template $SSO_CONFIG/dummySamlKeystore.jks
#config dir:
cp $SSO_CONFIG_TEMPLATES/aleph2_web_sso.properties_template $SSO_CONFIG/aleph2_web_sso.properties
cp $SSO_CONFIG_TEMPLATES/shiro.ini_template $SSO_CONFIG/shiro.ini

###############################################
#
# Build the RPM

tar --exclude='.gitignore' -czvf ${RPM_NAME}.tar.gz $RPM_BUILD_DIR/

mkdir rpmbuild
cd rpmbuild
mkdir BUILD  BUILDROOT  RPMS  SOURCES  SPECS  SRPMS
export TOPDIR=$(pwd)
cd ..
cp ${RPM_NAME}.tar.gz rpmbuild/SOURCES/
cp $RPM_SOURCE_DIR/SPECS/aleph2-webapps.spec rpmbuild/SPECS/
cd rpmbuild/SPECS/
rpmbuild --define "_topdir $TOPDIR" --define "_VERSION ${VERSION}" --define "_RELEASE ${RELEASETYPE}" -ba aleph2-webapps.spec

