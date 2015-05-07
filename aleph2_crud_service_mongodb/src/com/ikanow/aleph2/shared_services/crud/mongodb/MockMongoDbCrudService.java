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
 ******************************************************************************/
package com.ikanow.aleph2.shared_services.crud.mongodb;
import java.util.Optional;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.github.fakemongo.Fongo;
import com.ikanow.aleph2.data_model.objects.shared.AuthorizationBean;
import com.ikanow.aleph2.data_model.objects.shared.ProjectBean;

public class MockMongoDbCrudService<O, K> extends MongoDbCrudService<O, K> {
	
	public MockMongoDbCrudService(@NonNull String mock_name, @NonNull String db_name, @NonNull String coll_name, 
			@NonNull Class<O> bean_clazz, @NonNull Class<K> key_clazz, Optional<String> auth_fieldname, Optional<AuthorizationBean> auth, Optional<ProjectBean> project) {
		super(bean_clazz, key_clazz, new Fongo(mock_name).getDB(db_name).getCollection(coll_name), auth_fieldname, auth, project);
	}
}
