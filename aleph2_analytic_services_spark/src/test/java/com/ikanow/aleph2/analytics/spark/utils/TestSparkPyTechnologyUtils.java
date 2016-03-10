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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.google.common.io.Resources;

import fj.data.Either;

/**
 * @author Alex
 *
 */
public class TestSparkPyTechnologyUtils {

	@Test
	public void test_writeAleph2DriverZip() throws IOException {
		final String tmp_dir = System.getProperty("java.io.tmpdir");
		final String signature = "alex_test";
		final String expected_name = tmp_dir + "/aleph2_driver_py_" + signature + ".zip";
		final File f = new File(expected_name);
		try {
			f.delete();
		}
		catch (Exception e) {}
		assertTrue(!f.exists());
		
		assertEquals(expected_name, SparkPyTechnologyUtils.writeAleph2DriverZip(signature));
		
		assertTrue(f.exists());		
	}
	
	@Test
	public void test_writeUserPythonScriptTmpFile() throws IOException {
		{
			final String tmp_dir = System.getProperty("java.io.tmpdir");
			final String signature = "alex_test";
			final String expected_name = tmp_dir + "/user_py_script" + signature + ".py";
			final String test_script = "hello\nthere\n";
			final File f = new File(expected_name);
			try {
				f.delete();
			}
			catch (Exception e) {}
			assertTrue(!f.exists());
			
			assertEquals(expected_name, SparkPyTechnologyUtils.writeUserPythonScriptTmpFile(signature, Either.left(test_script)));
			
			assertTrue(f.exists());
			assertEquals(test_script, FileUtils.readFileToString(f));
		}
		{
			final String tmp_dir = System.getProperty("java.io.tmpdir");
			final String signature = "alex_test";
			final String expected_name = tmp_dir + "/user_py_script" + signature + ".py";
			final File f = new File(expected_name);
			try {
				f.delete();
			}
			catch (Exception e) {}
			assertTrue(!f.exists());
			
			assertEquals(expected_name, SparkPyTechnologyUtils.writeUserPythonScriptTmpFile(signature, Either.right("low_level_sample.py")));
			
			final String test_result = Resources.toString(Resources.getResource("low_level_sample.py"), Charsets.UTF_8);
			assertTrue(test_result.contains("SparkContext"));
			
			assertTrue(f.exists());
			assertEquals(test_result, FileUtils.readFileToString(f));			
		}
	}
	
}
