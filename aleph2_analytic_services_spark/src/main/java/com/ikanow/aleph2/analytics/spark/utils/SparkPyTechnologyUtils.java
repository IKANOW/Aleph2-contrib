/*******************************************************************************
 * Copyright 2016, The IKANOW Open Source Project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package com.ikanow.aleph2.analytics.spark.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.io.Resources;
import com.ikanow.aleph2.analytics.spark.services.SparkPyWrapperService;
import com.ikanow.aleph2.data_model.utils.Lambdas;

import fj.data.Either;

/** This class acts as a wrapper for Spark/Python related functionality
 *  Note these aren't static methods to make the python libraries life easy
 * @author Alex
 *
 */
public class SparkPyTechnologyUtils {
	
	/** Create a zip containing the Aleph2 driver
	 * @param bucket
	 * @param job
	 * @throws IOException
	 */
	public static String writeAleph2DriverZip(final String signature) throws IOException {
		final String tmp_dir = System.getProperty("java.io.tmpdir");
		final String filename = tmp_dir + "/aleph2_driver_py_" + signature + ".zip";
		
		final URL url = Resources.getResource("aleph2_driver.py");
		final String text = Resources.toString(url, Charsets.UTF_8);		
		final FileOutputStream fout = new FileOutputStream(filename);
		final ZipOutputStream zout = new ZipOutputStream(fout);
		final ZipEntry ze= new ZipEntry("aleph2_driver.py");
		zout.putNextEntry(ze);
		zout.write(text.getBytes());
		zout.closeEntry();
		zout.close();
		
		return filename;
	}
	
	/** Write the script to a temp file
	 * @param bucket
	 * @param job
	 * @param script
	 * @throws IOException 
	 */
	public static String writeUserPythonScriptTmpFile(final String signature, final Either<String, String> script_or_resource) throws IOException {
		final String tmp_dir = System.getProperty("java.io.tmpdir");
		final String filename = tmp_dir + "/user_py_script" + signature + ".py";
		
		final String script = script_or_resource.<String>either(l -> l, fj.data.Java8.Function_F(Lambdas.wrap_u(r -> Resources.toString(Resources.getResource(r), Charsets.UTF_8))));
		
		FileUtils.writeStringToFile(new File(filename), script);
		
		return filename;		
	}
	
	/** Returns a spark wrapper for use by python
	 * @param spark_context
	 * @param signature
	 * @param test_signature
	 * @return
	 */
	public SparkPyWrapperService getAleph2(JavaSparkContext spark_context, String signature, String test_signature) {
		return new SparkPyWrapperService(spark_context, signature, test_signature);
	}
	
	/** Returns a spark wrapper for use by python
	 * @param spark_context
	 * @param signature
	 * @return
	 */
	public SparkPyWrapperService getAleph2(JavaSparkContext spark_context, String signature) {
		return new SparkPyWrapperService(spark_context, signature);
	}
	
}
