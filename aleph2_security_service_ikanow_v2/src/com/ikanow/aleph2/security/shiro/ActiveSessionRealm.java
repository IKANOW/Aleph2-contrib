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
package com.ikanow.aleph2.security.shiro;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AccountException;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.bson.types.ObjectId;

import scala.Tuple2;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.ErrorUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;
import com.ikanow.aleph2.security.interfaces.IClearableRealmCache;
import com.ikanow.aleph2.security.interfaces.IRoleProvider;
import com.ikanow.aleph2.security.service.CoreEhCacheManager;

/** 
 * Class checks against an active session. =Functionality might not be necessary if the SessionDAO works.
 * @author jfreydank
 *
 */
public class ActiveSessionRealm extends AuthorizingRealm implements IClearableRealmCache {
	private static final Logger logger = LogManager.getLogger(ActiveSessionRealm.class);

	
	protected final IServiceContext _context;

//	protected IRoleProvider roleProvider;


	private Set<IRoleProvider> roleProviders;
	
	@Inject
	public ActiveSessionRealm(final IServiceContext service_context, CredentialsMatcher matcher, Set<IRoleProvider> roleProviders) {		
		super(CoreEhCacheManager.getInstance().getCacheManager(),matcher);
		_context = service_context;
		this.roleProviders = roleProviders;
		logger.debug("IkanowV1Realm name="+getName());
	}
	

	 
    /**
     * This implementation of the interface expects the principals collection to return a String username keyed off of
     * this realm's {@link #getName() name}
     *
     * @see #getAuthorizationInfo(org.apache.shiro.subject.PrincipalCollection)
     */
    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {

        //null usernames are invalid
        if (principals == null) {
            throw new AuthorizationException("PrincipalCollection method argument cannot be null.");
        }

        String username = (String) getAvailablePrincipal(principals);
        Set<String> roles = new HashSet<String>();
        Set<String> permissions = new HashSet<String>();
        
        for (IRoleProvider roleProvider : roleProviders) {
        	Tuple2<Set<String>, Set<String>> t2 = roleProvider.getRolesAndPermissions(username);
        	roles.addAll(t2._1());
            permissions.addAll(t2._2());        
        }  // for      

        SimpleAuthorizationInfo info = new SimpleAuthorizationInfo(roles);
        info.addStringPermissions(permissions);        
        return info;

    }


    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {

        UsernamePasswordToken upToken = (UsernamePasswordToken) token;
        String username = upToken.getUsername();

        // Null username is invalid
        if (username == null) {
            throw new AccountException("Null usernames are not allowed by this realm.");
        }

        AuthenticationInfo info = null;
/*        try {
        

    		SingleQueryComponent<AuthenticationBean> queryUsername = CrudUtils.anyOf(AuthenticationBean.class).when("username",username);		
            @SuppressWarnings("unused")
			ObjectId objectId = null;
	        try {
	            objectId = new ObjectId(username);
	            // if we get here the username can be mapped to an id
	           queryUsername = queryUsername.when("profileId",new ObjectId(username));
			} catch (Exception e) {
				// TODO: handle exception
			}
        
        Optional<AuthenticationBean> result = getAuthenticationStore().getObjectBySpec(queryUsername).get();
        if(result.isPresent()){
        	AuthenticationBean b = result.get();
        	logger.debug("Loaded user info from db:"+b);
			info = new IkanowV1AuthenticationInfo(b);
        }
        } catch (Throwable e) {
            final String message = "There was a Connection error while authenticating user [" + username + "]";
			logger.error(ErrorUtils.getLongForm(message+" {0}", e));

            // Rethrow any errors as an authentication exception
            throw new AuthenticationException(message, e);
        } finally {
            // TODO close connection?
        }
*/
        return info;
    }
    
    @Override
    public void clearAuthorizationCached(Collection<String> principalNames){
   	 logger.debug("clearCachedAuthorizationInfo for "+principalNames);
   	 SimplePrincipalCollection principals = new SimplePrincipalCollection(principalNames, this.getClass().getName());
   	 super.doClearCache(principals);   	 
    }

    @Override
    public void clearAllCaches(){
		 logger.debug("clearAllCaches");
			

		 Cache<Object, AuthenticationInfo> ac = getAuthenticationCache();
			if(ac!=null){
				ac.clear();
			}
			Cache<Object, AuthorizationInfo> ar = getAuthorizationCache();
			if(ar!=null){
				ar.clear();
			}		
    }
     
}
