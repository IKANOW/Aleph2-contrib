if [ "${bamboo_skip_unit_tests}" = "true" ]; then
    echo "Generating dummy report"
    mkdir -p surefire-reports
    cat <<EOT > surefire-reports/junit_dummy.xml
<?xml version="1.0" encoding="UTF-8"?>
<testsuites>
   <testsuite name="JUnitXmlReporter" errors="0" tests="0" failures="0" time="0" timestamp="2013-05-24T10:23:58" />
   <testsuite name="JUnitXmlReporter.constructor" errors="0" skipped="1" tests="1" failures="0" time="0.006" timestamp="2013-05-24T10:23:58">
      <properties>
         <property name="java.vendor" value="Sun Microsystems Inc." />
         <property name="compiler.debug" value="on" />
         <property name="project.jdk.classpath" value="jdk.classpath.1.6" />
      </properties>
      <testcase classname="JUnitXmlReporter.constructor" name="should default consolidate to true" time="0">
         <skipped />
      </testcase>
   </testsuite>
</testsuites>    
EOT
fi
