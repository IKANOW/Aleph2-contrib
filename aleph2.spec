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
%setup -n %{name}-%{_VERSION}-%{_RELEASE}
mkdir -p %{_buildrootdir}/%{name}-%{_VERSION}-%{_RELEASE}.x86_64/
cp -rv %{_builddir}/%{name}-%{_VERSION}-%{_RELEASE}/* %{_buildrootdir}/%{name}-%{_VERSION}-%{_RELEASE}.x86_64/

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
/opt/ikanow/etc/
