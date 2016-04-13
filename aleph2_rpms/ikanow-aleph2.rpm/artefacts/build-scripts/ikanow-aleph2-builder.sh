export M2_HOME=/opt/maven-3.0/

# Full testless install of core
cd Aleph2
$M2_HOME/bin/mvn -T 4 -B package -DskipTests=true

# Full testless install of core-technology
cd ../Aleph2-contrib
$M2_HOME/bin/mvn -T 4 -B package -DskipTests=true

if [ "${bamboo.skip_unit_tests}" != "true" ]; then
    # Run all core tests
    cd ../Aleph2
    export PROJECTS=$($M2_HOME/bin/mvn dependency:tree | grep "\[INFO\] aleph2_[^.]*$" | grep -o "aleph2_.*")
    echo "*** List of projects to test in parallel = $PROJECTS"
    echo $PROJECTS | xargs -n 1 -P 4 $M2_HOME/bin/mvn -B test --projects || true &

    # Run all core-technology tests
    cd ../Aleph2-contrib
    export PROJECTS=$($M2_HOME/bin/mvn dependency:tree | grep "\[INFO\] aleph2_[^.]*$" | grep -o "aleph2_.*")
    echo "*** List of projects to test in parallel = $PROJECTS"
    echo $PROJECTS | xargs -n 1 -P 4 $M2_HOME/bin/mvn -B test --projects || true &

    wait
fi