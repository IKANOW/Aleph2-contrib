package com.ikanow.aleph2.security.service;

import java.sql.Connection;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.authc.AccountException;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.util.ByteSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.ICrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IManagementCrudService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.data_model.objects.data_import.DataBucketBean;
import com.ikanow.aleph2.data_model.utils.CrudUtils;
import com.ikanow.aleph2.data_model.utils.CrudUtils.SingleQueryComponent;

public class IkanowV1Realm extends AuthorizingRealm {
	private static final Logger logger = LogManager.getLogger(IkanowV1Realm.class);

	
	protected final IServiceContext _context;
	protected IManagementDbService _core_management_db = null;
	protected IManagementDbService _underlying_management_db = null;

	private ICrudService<AuthenticationBean> v1_db = null;
	
	@Inject
	public IkanowV1Realm(final IServiceContext service_context) {		
		super();
		_context = service_context;
	}
	
	protected ICrudService<AuthenticationBean> getDb(){
		if(v1_db == null){
		_core_management_db = _context.getCoreManagementDbService();		
		_underlying_management_db = _context.getService(IManagementDbService.class, Optional.empty()).get();
		String options = "security.authentication/"+AuthenticationBean.class.getName();
	      v1_db = _underlying_management_db.getUnderlyingPlatformDriver(ICrudService.class, Optional.of(options)).get();
		}
	      return v1_db;		
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

        Connection conn = null;
        Set<String> roleNames = null;
        Set<String> permissions = null;
/*        try {
            conn = dataSource.getConnection();

            // Retrieve roles and permissions from database
            roleNames = getRoleNamesForUser(conn, username);
            if (permissionsLookupEnabled) {
                permissions = getPermissions(conn, username, roleNames);
            }

        } catch (SQLException e) {
            final String message = "There was a SQL error while authorizing user [" + username + "]";
            if (log.isErrorEnabled()) {
                log.error(message, e);
            }

            // Rethrow any SQL errors as an authorization exception
            throw new AuthorizationException(message, e);
        } finally {
            JdbcUtils.closeConnection(conn);
        }
*/
        SimpleAuthorizationInfo info = new SimpleAuthorizationInfo(roleNames);
        info.setStringPermissions(permissions);
        return info;

    }


    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {

        UsernamePasswordToken upToken = (UsernamePasswordToken) token;
        String username = upToken.getUsername();

        // Null username is invalid
        if (username == null) {
            throw new AccountException("Null usernames are not allowed by this realm.");
        }

        SimpleAuthenticationInfo info = null;
        try {
        

        
		SingleQueryComponent<AuthenticationBean> queryUsername = CrudUtils.anyOf(AuthenticationBean.class).when("username",username);		
        Optional<AuthenticationBean> result = getDb().getObjectBySpec(queryUsername).get();
        if(result.isPresent()){
        	AuthenticationBean b = result.get();
        	logger.debug("Loaded user info from db:"+b);
/*
        Connection conn = null;
        String password = null;
        String salt = null;
        
            conn = dataSource.getConnection();

            String password = null;
            String salt = null;
            switch (saltStyle) {
            case NO_SALT:
                password = getPasswordForUser(conn, username)[0];
                break;
            case CRYPT:
                // TODO: separate password and hash from getPasswordForUser[0]
                throw new ConfigurationException("Not implemented yet");
                //break;
            case COLUMN:
                String[] queryResults = getPasswordForUser(conn, username);
                password = queryResults[0];
                salt = queryResults[1];
                break;
            case EXTERNAL:
                password = getPasswordForUser(conn, username)[0];
                salt = getSaltForUser(username);
            }

            if (password == null) {
                throw new UnknownAccountException("No account found for user [" + username + "]");
            }


        	
			info = new SimpleAuthenticationInfo(username, password.toCharArray(), getName());
            
            if (salt != null) {
                info.setCredentialsSalt(ByteSource.Util.bytes(salt));
            }
*/
        }
        } catch (Exception e) {
            final String message = "There was a Connection error while authenticating user [" + username + "]";
            logger.error(message,e);

            // Rethrow any SQL errors as an authentication exception
            throw new AuthenticationException(message, e);
        } finally {
            //JdbcUtils.closeConnection(conn);
        }

        return info;
    }
    

}
