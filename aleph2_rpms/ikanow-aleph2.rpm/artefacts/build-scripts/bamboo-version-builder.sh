export VERSION="2.${bamboo.a2_minor_version_NIGHTLY}.$(date +%y%j%H).${bamboo.buildNumber}"
if [ "$(date --utc +%H)" = "07" ]; then
    export RELEASETYPE="nightly"
elif [ "${bamboo.skip_unit_tests}" = "true" ]; then
    export RELEASETYPE="untested"
else
    export RELEASETYPE="ci"
fi
