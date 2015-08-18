package com.ikanow.aleph2.security.module;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.security.service.IkanowV1Realm;

public class IkanowV1SecurityModule extends CoreSecurityModule{
	
	
	protected IServiceContext serviceContext;
	public IkanowV1SecurityModule(IServiceContext serviceContext){
		this.serviceContext = serviceContext;
	}
	
	@Override
	protected void bindRealms() {
		super.bindRealms();
		bindRealm().toInstance(new IkanowV1Realm(serviceContext));
	}

}
