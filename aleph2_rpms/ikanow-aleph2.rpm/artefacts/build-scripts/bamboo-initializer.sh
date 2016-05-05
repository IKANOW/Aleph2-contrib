export BUILD_HOME=/home/bamboo/ikos-build
rm -rf $BUILD_HOME
mkdir -p $BUILD_HOME
ln -sf $BUILD_HOME

#(these aren't needed any more)
#rm -rf *.tgz 
#rm -rf *.gz 
#rm -rf dist
#rm -rf ikos-core
#rm -rf ikos-core-technology
#rm -rf rpmbuild/
#rm -rf ikanow-aleph2-*

echo "skip_unit_tests=$bamboo_skip_unit_tests" > skip_unit_tests.txt
if [ "$bamboo_skip_unit_tests" = "true" ]; then
    echo "Skipping unit tests..."
    if [ "$(date --utc +%H)" = "07" ]; then
        echo "NOT ALLOWED TO SKIP TESTS IN THE ACTUAL NIGHTLY - OVERWRITING"
        echo 'skip_unit_tests=false' > skip_unit_tests.txt
    fi
fi