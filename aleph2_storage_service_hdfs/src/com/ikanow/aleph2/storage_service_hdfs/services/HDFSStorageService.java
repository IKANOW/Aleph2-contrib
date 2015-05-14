package com.ikanow.aleph2.storage_service_hdfs.services;
import java.util.Optional;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.ikanow.aleph2.data_model.interfaces.data_services.IStorageService;

public class HDFSStorageService implements IStorageService {

	private static final Logger logger = Logger.getLogger(HDFSStorageService.class);

	@Override
	public String getRootPath() {
		return "/";
	}

	@Override
	public <T> @NonNull T getUnderlyingPlatformDriver(
			@NonNull Class<T> driver_class, Optional<String> driver_options) {
		T driver = null;
		
		if(driver_class!=null){
			try {
				driver = driver_class.newInstance();
			} catch (Exception e) {
				logger.error("Error instamciating driver class",e);
			}
		}
		return driver;
	}

}
