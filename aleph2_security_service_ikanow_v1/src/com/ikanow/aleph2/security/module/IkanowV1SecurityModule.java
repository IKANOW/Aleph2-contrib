package com.ikanow.aleph2.security.module;

import org.apache.shiro.authc.credential.CredentialsMatcher;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.security.service.IkanowV1CredentialsMatcher;
import com.ikanow.aleph2.security.service.IkanowV1Realm;

public class IkanowV1SecurityModule extends CoreSecurityModule{
	
	
	public IkanowV1SecurityModule(){
	}
	
	@Override
	protected void bindRealms() {
		super.bindRealms();
		
		try {
			bind(CredentialsMatcher.class).to(IkanowV1CredentialsMatcher.class);
			bindRealm().toConstructor(IkanowV1Realm.class.getConstructor(IServiceContext.class, CredentialsMatcher.class));
			
		} catch (NoSuchMethodException | SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
