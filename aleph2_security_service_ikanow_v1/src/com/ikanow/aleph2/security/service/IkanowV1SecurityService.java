package com.ikanow.aleph2.security.service;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.security.module.IkanowV1SecurityModule;

public class IkanowV1SecurityService extends SecurityService{
	
	
	@Inject
	public IkanowV1SecurityService(IServiceContext serviceContext) {
		super(serviceContext);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected Injector getInjector() {
		Injector injector = Guice.createInjector(new IkanowV1SecurityModule(serviceContext));
		return injector;
	}
	
}
