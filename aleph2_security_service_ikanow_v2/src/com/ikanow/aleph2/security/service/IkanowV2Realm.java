package com.ikanow.aleph2.security.service;

import org.apache.shiro.realm.ldap.JndiLdapRealm;

import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;


public class IkanowV2Realm extends JndiLdapRealm {

	
	public IkanowV2Realm(){		
		super();		
	}
}
