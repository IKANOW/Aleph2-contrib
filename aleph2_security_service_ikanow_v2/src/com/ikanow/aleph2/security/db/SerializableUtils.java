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
package com.ikanow.aleph2.security.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.codec.Base64;


public class SerializableUtils {
	protected static final Logger logger = LogManager.getLogger(SerializableUtils.class);

    public static String serialize(Object raw) {
    	String str = null;
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(raw);
            str = Base64.encodeToString(bos.toByteArray());
        } catch (Exception e) {
        	logger.error("Caught exception serializing:",e);
        }
        return str;
    }
    public static Object deserialize(String str) {
    	Object value = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(str));
            ObjectInputStream ois = new ObjectInputStream(bis);
            value =  ois.readObject();
        } catch (Exception e) {
        	logger.error("Caught exception deserializing:",e);
        }
        return value;
        
    }
}
