from pyspark import SparkContext
from pyspark.rdd import RDD

class Aleph2Driver:
	"""A python driver for Aleph2"""
	
	def __init__(self, sc, signature, test_signature):
		self.aleph2 = sc._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass("com.ikanow.aleph2.analytics.spark.utils.SparkPyTechnologyUtils").newInstance().getAleph2(sc._jsc, signature, test_signature)
        
	def __init__(self, sc, signature):
		self.aleph2 = sc._jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass("com.ikanow.aleph2.analytics.spark.utils.SparkPyTechnologyUtils").newInstance().getAleph2(sc._jsc, signature)
	
	def getRddInputNames(self):
		return self.aleph2.getRddInputNames()
		
	def getRddInput(self, sc, name):
		return RDD(sc._jvm.SerDe.javaToPython(self.aleph2.getRddInput(name)), sc)
		
	def getAllRddInputs(self, sc):
		return RDD(sc._jvm.SerDe.javaToPython(self.aleph2.getAllRddInputs()), sc)
		
	def emitRdd(self, rdd):
		return self.aleph2.emitRdd(rdd._to_java_object_rdd())
		
	def externalEmitRdd(self, path, rdd):
		return self.aleph2.externalEmitRdd(path, rdd._to_java_object_rdd())

	def emitObject(self, obj):
		return self.aleph2.emitObject(obj)
		
	def externalEmitObject(self, path, obj):
		return self.aleph2.emitObject(path, obj)
