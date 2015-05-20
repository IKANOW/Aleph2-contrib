package com.ikanow.aleph2.storage_service_hdfs.services;

import static org.junit.Assert.*;

import java.util.Optional;

import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;

public class HDFSStorageSystemTest {
	
	@Test
	public void test(){
			HDFSStorageService storageService = new HDFSStorageService();
			
			FileContext fs1 = storageService.getUnderlyingPlatformDriver(FileContext.class,Optional.<String>empty());
			assertNotNull(fs1);
			AbstractFileSystem fs2 = storageService.getUnderlyingPlatformDriver(AbstractFileSystem.class, Optional.<String>empty());
			assertNotNull(fs2);
			RawLocalFileSystem fs3 = storageService.getUnderlyingPlatformDriver(org.apache.hadoop.fs.RawLocalFileSystem.class,Optional.<String>empty());
			assertNotNull(fs3); 
	}

}
