package com.ikanow.aleph2.security.service;

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;

public class IkanowV1AuthenticationInfo implements AuthenticationInfo {

	
	private AuthenticationBean authenticationBean;
	public AuthenticationBean getAuthenticationBean() {
		return authenticationBean;
	}
	private SimplePrincipalCollection principalCollection;

	public IkanowV1AuthenticationInfo(AuthenticationBean ab){
		this.authenticationBean = ab;
		this.principalCollection =  new SimplePrincipalCollection(ab.getUsername(),IkanowV1Realm.class.getSimpleName());
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = -8123608158013803489L;

	@Override
	public PrincipalCollection getPrincipals() {
		
		return principalCollection;
	}

	@Override
	public Object getCredentials() {
		return authenticationBean.getPassword();
	}

}
