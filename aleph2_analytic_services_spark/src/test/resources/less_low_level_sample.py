from pyspark import SparkContext
from pyspark import SparkConf
import sys
from aleph2_driver import Aleph2Driver

def update_dictionary(map):
        map.update(test="alex")
        return map

if __name__ == "__main__":
        sc = SparkContext(appName="alex_test_app")
        aleph2 = Aleph2Driver(sc, sys.argv[1])
        print aleph2.getRddInputNames()
        to_output = aleph2.getAllRddInputs().map(lambda m: update_dictionary(m))
        #to_output = aleph2.getRddInput("netflow").map(lambda m: update_dictionary(m))
        print aleph2.emitRdd(to_output)
