package com.ikanow.aleph2.analytics.storm.services;

import java.util.Arrays;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.ikanow.aleph2.analytics.storm.data_model.IStormController;
import com.ikanow.aleph2.analytics.storm.modules.MockStormAnalyticTechnologyModule;

public class MockStormAnalyticTechnologyService extends StormAnalyticTechnologyService {

	/** Guice constructor
	 */
	@Inject
	public MockStormAnalyticTechnologyService(final IStormController storm_controller) {
		super(storm_controller);
	}		
	
	/** This service needs to load some additional classes via Guice. Here's the module that defines the bindings
	 * @return
	 */
	public static List<Module> getExtraDependencyModules() {
		return Arrays.asList((Module)new MockStormAnalyticTechnologyModule());
	}
	
	@Override
	public void youNeedToImplementTheStaticFunctionCalled_getExtraDependencyModules() {
		//(done see above)
	}
}
