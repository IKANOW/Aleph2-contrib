export M2_HOME=/opt/maven-3.0/

# Set version numbers
sh ikos-core-technology/aleph2_rpms/ikanow-aleph2.rpm/artefacts/build-scripts/ikanow-aleph2-global-version-updater.sh ${VERSION}-${RELEASETYPE}

# Full testless install of core
cd ikos-core
$M2_HOME/bin/mvn -T 4 -B clean install package -Daleph2.scope=provided -DskipTests=true || exit -1

# Full testless install of core-technology
cd ../ikos-core-technology
$M2_HOME/bin/mvn -T 4 -B clean install package -Daleph2.scope=provided -DskipTests=true || exit -1

# Full testless install of core-apps
cd ../ikos-core-apps
$M2_HOME/bin/mvn -T 4 -B clean install package -Daleph2.scope=provided -DskipTests=true || exit -1

#problem ones: CDS / data_import_manager
export PROBLEM_PROJECTS_CORE="aleph2_core_distributed_services_library|aleph2_data_import_manager"
export PROBLEM_PROJECTS_TECH="aleph2_analytic_services_spark"
export PROBLEM_PROJECTS_APPS="__NONE__"

#(split this up into 3 so can run in 3 different containers)
if [ "${bamboo_skip_unit_tests}" != "true" ]; then
    # Run all core tests
    cd ../ikos-core
    export PROJECTS=$($M2_HOME/bin/mvn dependency:tree | grep "\[INFO\] aleph2_[^.]*$" | grep -o "aleph2_.*" | grep -v -P "$PROBLEM_PROJECTS_CORE")
    echo "*** List of projects to test in parallel = $PROJECTS"
    echo $PROJECTS | xargs -n 1 -P 4 $M2_HOME/bin/mvn -B -Daleph2.scope=provided -DskipTests=false test --projects || true &

    wait

    # Run all core-technology tests
    cd ../ikos-core-technology
    export PROJECTS=$($M2_HOME/bin/mvn dependency:tree | grep "\[INFO\] aleph2_[^.]*$" | grep -o "aleph2_.*" | grep -v -P "$PROBLEM_PROJECTS_TECH")
    echo "*** List of projects to test in parallel = $PROJECTS"
    echo $PROJECTS | xargs -n 1 -P 4 $M2_HOME/bin/mvn -B -Daleph2.scope=provided -DskipTests=false test --projects || true &

    wait

        # Run all core-technology tests
    cd ../ikos-core-apps
    export PROJECTS=$($M2_HOME/bin/mvn dependency:tree | grep "\[INFO\] aleph2_[^.]*$" | grep -o "aleph2_.*" | grep -v -P "$PROBLEM_PROJECTS_APPS")
    echo "*** List of projects to test in parallel = $PROJECTS"
    echo $PROJECTS | xargs -n 1 -P 4 $M2_HOME/bin/mvn -B -Daleph2.scope=provided -DskipTests=false test --projects || true &

    wait

    #Now do the problem ones on their own:
    #(later on could try xargs and doing some limited parallelism?)
    cd ../ikos-core
    $M2_HOME/bin/mvn -B -Daleph2.scope=provided -DskipTests=false test --projects aleph2_core_distributed_services_library &
    $M2_HOME/bin/mvn -B -Daleph2.scope=provided -DskipTests=false test --projects aleph2_data_import_manager &
    wait
    
    cd ../ikos-core-technology
    #(Spark is a special case - run twice, the first time is spark 2.10, the second is spark 2.11 ... and then rebuild!)
    $M2_HOME/bin/mvn -B -Daleph2.scope=provided -DskipTests=false test --projects aleph2_analytic_services_spark
    $M2_HOME/bin/mvn -B -Daleph2.scope=provided -DskipTests=false clean test --projects aleph2_analytic_services_spark
    $M2_HOME/bin/mvn -B -Daleph2.scope=provided -DskipTests=true clean package --projects aleph2_analytic_services_spark 

    cd ../ikos-core-apps
    #(no problem apps projects currently identified)
    
    #Ugh need to rebuild everything because of all the cleaning
fi
