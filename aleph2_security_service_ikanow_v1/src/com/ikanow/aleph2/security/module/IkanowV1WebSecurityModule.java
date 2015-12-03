package com.ikanow.aleph2.security.module;



public class IkanowV1WebSecurityModule extends IkanowV1SecurityModule {

	@Override
	public void configure() {
		configureShiro();
		
	}


	@Override
	protected void bindRealms() {
		// TODO Auto-generated method stub
		super.bindRealms();
	}

	@Override
	protected void bindRoleProviders() {
		// TODO Auto-generated method stub
		super.bindRoleProviders();
	}

	

}
