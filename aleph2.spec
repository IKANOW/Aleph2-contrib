Summary: IKANOW Aleph2 Plugin
Name: ikanow-aleph2
Version: %{_VERSION}
Release: %{_RELEASE}
Requires: curl dos2unix 
License: None
Group: ikanow
BuildArch: noarch
Prefix: /mnt/opt
Source: %{name}-%{_VERSION}-%{_RELEASE}.tar.gz

%description
IKANOW Aleph2 Plugins 

###########################################################################
# SCRIPTLETS, IN ORDER OF EXECUTION
%prep
%setup
mkdir -p %{_buildrootdir}/%{name}-%{_VERSION}-%{_RELEASE}.x86_64/
cp -rv %{_builddir}/%{name}-%{_VERSION}/* %{_buildrootdir}/%{name}-%{_VERSION}-%{_RELEASE}.x86_64/

%pre

###########################################################################
# Check to make sure that JDK8 is installed and in use
 
	if ! readlink -f /usr/java/default | grep -q '^/usr/java/jdk1.8'; then 
		echo "***ERROR: Aleph2 requires JDK1.8 to be installed in /usr/java (eg via RPM)"
		exit -1
	fi
	
%install
###########################################################################
# INSTALL *AND* UPGRADE
	# (All files created from the tarball)

%post
###########################################################################
# INSTALL *AND* UPGRADE

%preun

%postun
###########################################################################
# (Nothing to do)

%posttrans
###########################################################################
# FILE LISTS

%files
%defattr(-,tomcat,tomcat)
/opt/ikanow/lib/
#/aleph2_access_library_es_proxy-%{_VERSION}-%{_RELEASE}-shaded.jar
#aleph2_access_manager-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_core_distributed_services_library-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_crud_service_elasticsearch-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_crud_service_mongodb-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_data_analytics_manager-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_data_import_manager-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_data_model-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_harvest_context_library-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_management_db_service-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_management_db_service_mongodb-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_object_import_library-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_search_index_service_elasticsearch-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_security_service_ikanow_v1-${_VERSION}-${_RELEASE}-shaded.jar
#aleph2_storage_service_hdfs-${_VERSION}-${_RELEASE}-shaded.jar

