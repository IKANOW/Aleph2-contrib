/*******************************************************************************
 * Copyright 2015, The IKANOW Open Source Project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.ikanow.aleph2.analytics.storm.modules;

import com.google.inject.AbstractModule;
import com.ikanow.aleph2.analytics.storm.data_model.IStormController;
import com.ikanow.aleph2.analytics.storm.services.LocalStormController;

/** Defines guice dependencies
 *  NO TEST COVERAGE - TEST BY HAND IF CHANGED
 * @author Alex
 */
public class MockStormAnalyticTechnologyModule extends AbstractModule {

	private static IStormController _controller;
	
	/* (non-Javadoc)
	 * @see com.google.inject.AbstractModule#configure()
	 */
	@Override
	public void configure() {
		this.bind(IStormController.class).toInstance(getController());
	}
	
	/** Initializes the storm instance
	 * @return a local storm controller 
	 */
	public static IStormController getController() {
		synchronized (IStormController.class) {
			if (null != _controller) {
				return _controller;
			}
			return (_controller = new LocalStormController());
		}
	}
	
}
