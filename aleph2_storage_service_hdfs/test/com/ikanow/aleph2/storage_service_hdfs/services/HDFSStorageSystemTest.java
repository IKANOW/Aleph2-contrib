package com.ikanow.aleph2.storage_service_hdfs.services;

import static org.junit.Assert.*;

import java.util.Optional;

import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;

public class HDFSStorageSystemTest {
	
	@Test
	public void test(){
			HDFSStorageService storageService = new HDFSStorageService();
			
			AbstractFileSystem fs1 = storageService.getUnderlyingPlatformDriver(AbstractFileSystem.class, Optional.<String>empty());
			assertNotNull(fs1);
			RawLocalFileSystem fs2 = storageService.getUnderlyingPlatformDriver(org.apache.hadoop.fs.RawLocalFileSystem.class,Optional.<String>empty());
			assertNotNull(fs2);

	}

}
