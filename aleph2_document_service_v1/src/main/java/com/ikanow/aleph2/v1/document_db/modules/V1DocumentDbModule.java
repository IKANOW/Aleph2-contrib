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
package com.ikanow.aleph2.v1.document_db.modules;

import com.google.inject.AbstractModule;
import com.ikanow.aleph2.data_model.utils.BeanTemplateUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.ModuleUtils;
import com.ikanow.aleph2.data_model.utils.PropertiesUtils;
import com.ikanow.aleph2.v1.document_db.data_model.V1DocDbConfigBean;
import com.ikanow.infinit.e.data_model.Globals;
import com.ikanow.infinit.e.data_model.Globals.Identity;
import com.typesafe.config.Config;

/** Guice injection
 * @author Alex
 */
public class V1DocumentDbModule extends AbstractModule {

	@Override
	protected void configure() {
		final Config config = ModuleUtils.getStaticConfig();				
		V1DocDbConfigBean bean;
		try {
			bean = BeanTemplateUtils.from(PropertiesUtils.getSubConfig(config, V1DocDbConfigBean.PROPERTIES_ROOT).orElse(null), V1DocDbConfigBean.class);
		} 
		catch (Exception e) {
			throw new RuntimeException(ErrorUtils.get(ErrorUtils.INVALID_CONFIG_ERROR,
					V1DocDbConfigBean.class.toString(),
					config.getConfig(V1DocDbConfigBean.PROPERTIES_ROOT)
					), e);
		}
		this.bind(V1DocDbConfigBean.class).toInstance(bean); // (for crud service)
		
		//(While we're here, set up some global V1 constants)

		Globals.setIdentity(Identity.IDENTITY_SERVICE);
		Globals.overrideConfigLocation(bean.infinite_config_home());
	}
}

