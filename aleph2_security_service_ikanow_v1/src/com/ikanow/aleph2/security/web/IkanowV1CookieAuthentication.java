package com.ikanow.aleph2.security.web;

import java.security.SecureRandom;
import java.util.Date;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.data_services.IManagementDbService;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.WriteResult;


public class IkanowV1CookieAuthentication {
	public static IkanowV1CookieAuthentication instance = null;
	protected IServiceContext serviceContext = null;
	protected IManagementDbService _underlying_management_db = null;
	private DBCollection cookieDb = null; 
	private static final Logger logger = LogManager.getLogger(IkanowV1CookieAuthentication.class);

	private IkanowV1CookieAuthentication(IServiceContext serviceContext){
		this.serviceContext = serviceContext;
	}
	
	public static synchronized IkanowV1CookieAuthentication getInstance(Injector injector){
		if(instance == null){
			IServiceContext serviceContext = injector.getInstance(IServiceContext.class);
			instance =  new IkanowV1CookieAuthentication(serviceContext);
		}
		return instance;
	}
	
	@SuppressWarnings("unchecked")
	protected void initDb(){
		if(_underlying_management_db == null) {
		_underlying_management_db = serviceContext.getService(IManagementDbService.class, Optional.empty()).get();
		}
		String cookieOptions = "security.cookies";
		cookieDb = _underlying_management_db.getUnderlyingPlatformDriver(DBCollection.class, Optional.of(cookieOptions)).get();
		}

	protected DBCollection getCookieStore(){
		if(cookieDb == null){
			initDb();
		}
	      return cookieDb;		
	}

	public static String v1CookieAction(){
		String cookie = "";
		
//		if ( authuser != null )
//		{
			// Since logging-in isn't time critical, we'll ensure that api users have their api cookie at this point...
		/*	if (null != authuser.getApiKey()) {
				CookiePojo cp = new CookiePojo();
				cp.set_id(authuser.getProfileId());
				cp.setCookieId(cp.get_id());
				cp.setApiKey(authuser.getApiKey());
				cp.setStartDate(authuser.getCreated());
				cp.setProfileId(authuser.getProfileId());
				DbManager.getSocial().getCookies().save(cp.toDb());						 
			}//TESTED
*/
/*			if ((authuser.getAccountType() == null) ||
					!(authuser.getAccountType().equalsIgnoreCase("admin") || authuser.getAccountType().equalsIgnoreCase("admin-enabled")))
			{
				multi = false; // (not allowed except for admin)
			}*/

/*			CookieSetting cookieId = createSessionCookie(authuser.getProfileId(), true, response.getServerInfo().getPort());
			if (null != cookieId) {

				Series<CookieSetting> cooks = response.getCookieSettings();				 
				cooks.add(cookieId);
				response.setCookieSettings(cooks);
				isLogin = true;
				cookieLookup = cookieId.getValue();
				boolean bAdmin = false;
*/
				//If this request is checking admin status, check that
		/*		if (urlStr.contains("/admin/"))
				{
					isLogin = false;
					if (authuser.getAccountType().equalsIgnoreCase("admin")) {
						bAdmin = true;
						isLogin = true;
					}
					else if (authuser.getAccountType().equalsIgnoreCase("admin-enabled")) {
						isLogin = true;
						if (!multi) {
							authuser.setLastSudo(new Date());
							MongoDbManager.getSocial().getAuthentication().save(authuser.toDb());
							bAdmin = true;
						}
					}
				}//TESTED
*/
/*				logMsg.setLength(0);
				logMsg.append("auth/login");
				logMsg.append(" user=").append(user);
				logMsg.append(" userid=").append(authuser.getProfileId().toString());
				if (bAdmin) logMsg.append(" admin=true");
				logMsg.append(" success=").append(isLogin);
				logger.info(logMsg.toString());
				login_profile_id = authuser.getProfileId().toString();
				
			} */
	//	}

		return cookie;
	}
	
	/**
	 * Creates a new session cookie  for a user, adding
	 * an entry to our cookie table (maps cookieid
	 * to userid) and starts the clock
	 * 
	 * @param username
	 * @param bMulti if true lets you login from many sources
	 * @param bOverride if false will fail if already logged in
	 * @return
	 */
	public CookieBean createCookie(String userId)
	{
		deleteSessionCookieInDb(userId);
		CookieBean cookie = new CookieBean();
		ObjectId objectId = generateRandomId();
		
		cookie.set_id(objectId.toString()); 
		cookie.setCookieId(objectId.toString());
		Date now = new Date();
		cookie.setLastActivity(now);
		cookie.setProfileId(userId);
		cookie.setStartDate(now);
		saveSessionCookieInDb(cookie);

		return cookie;
		
	}
	private boolean saveSessionCookieInDb(CookieBean cookie) {
		int dwritten = 0;
		try {
			BasicDBObject query = new BasicDBObject();
			query.put("_id", new ObjectId(cookie.get_id()));
			query.put("profileId", new ObjectId(cookie.getProfileId()));
			query.put("cookieId", new ObjectId(cookie.getCookieId()));
			query.put("startDate", cookie.getStartDate());
			query.put("lastActivity", cookie.getLastActivity());
			if(cookie.getApiKey()!=null){
			 query.put("apiKey", cookie.getApiKey());
			}
			WriteResult result = getCookieStore().insert(query);
			dwritten = result.getN();
			
		} catch (Exception e) {
			logger.error("saveSessionCookieInDb caught exception",e);			
		}		
		return dwritten>0;
	}

	public static ObjectId generateRandomId() {
		SecureRandom randomBytes = new SecureRandom();
		byte bytes[] = new byte[12];
		randomBytes.nextBytes(bytes);
		return new ObjectId(bytes); 		
	}

	protected boolean deleteSessionCookieInDb(String userId){
		int deleted = 0;
		try {
			BasicDBObject query = new BasicDBObject();
			query.put("profileId", new ObjectId(userId));
			//deleteObjectsBySpec(CrudUtils.allOf(CookieBean.class).when(CookieBean::getProfileId, new ObjectId(userId))).get();
			WriteResult result = getCookieStore().remove(query);
			deleted = result.getN();
			
		} catch (Exception e) {
			logger.error("deleteSessionCookieInDb caught exception",e);			
		}
		return deleted>0;
	}


}
