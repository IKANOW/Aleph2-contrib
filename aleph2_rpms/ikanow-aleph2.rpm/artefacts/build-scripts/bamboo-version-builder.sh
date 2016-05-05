export VERSION="2.${bamboo_a2_minor_version_NIGHTLY}.$(date --utc +%y%j%H).${bamboo_buildNumber}"
if [ "${bamboo_release_type}" != "" ]; then
    export RELEASETYPE="${bamboo_release_type}"
elif [ "$(date --utc +%H)" = "07" ]; then
    export RELEASETYPE="nightly"
elif [ "${bamboo_skip_unit_tests}" = "true" ]; then
    export RELEASETYPE="untested"
else
    export RELEASETYPE="ci"
fi
