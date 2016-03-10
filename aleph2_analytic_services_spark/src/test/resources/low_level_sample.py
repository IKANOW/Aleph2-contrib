from pyspark import SparkContext
from pyspark import SparkConf
import sys
from pyspark.rdd import RDD

def update_dictionary(map):
        map.update(test="alex")
        return map

if __name__ == "__main__":
        print("here1")
        conf = SparkConf()
        sc = SparkContext(appName="alex_test_app")
        print("here2")
        print("here2b: " + conf.get("spark.aleph2_job_config"))
        aleph2 = sc._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass("com.ikanow.aleph2.analytics.spark.utils.SparkPyTechnologyUtils").newInstance().getAleph2(sc._jsc, sys.argv[1])
        print("here3")
        print aleph2.getRddInputNames()
        print("here4")
        #print RDD(sc._jvm.SerDe.javaToPython(aleph2.getAllRddInputs()), sc).count()
        print("here5")
        to_output = RDD(sc._jvm.SerDe.javaToPython(aleph2.getAllRddInputs()), sc).map(lambda m: update_dictionary(m))
        aleph2.emitRdd(to_output._to_java_object_rdd())
