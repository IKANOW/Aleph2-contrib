package com.ikanow.aleph2.security.service;

import com.google.inject.Injector;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;

public class IkanowV1CookieAuthentication {
	public static IkanowV1CookieAuthentication instance = null;
	protected IServiceContext serviceContext = null;
	
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
	
/*	private static CookieSetting createSessionCookie(ObjectId user, boolean bSet, int nClientPort)
	{
		//Create a new objectId to map this cookie to a userid
		String set = null;
		if (bSet) {
			ObjectId cookieId = RESTTools.createSession(user, multi, override);
			if (null == cookieId) { 
				return null;
			}
			set = cookieId.toString();
		}
		else {
			set = "";
		}
		CookieSetting cs = null;
		//store in mongo (or whatever db we need)
		try
		{
			cs = new CookieSetting("infinitecookie",set);
			cs.setPath("/");
			cs.setAccessRestricted(true);
			if ((443 == nClientPort) || (8443 == nClientPort)) {
				cs.setSecure(true);
			}
		}
		catch (Exception ex)
		{
			logger.error("Line: [" + ex.getStackTrace()[2].getLineNumber() + "] " + ex.getMessage());
		}
		return cs;
	}
	*/
	
	/**
	 * Creates a new session for a user, adding
	 * an entry to our cookie table (maps cookieid
	 * to userid) and starts the clock
	 * 
	 * @param username
	 * @param bMulti if true lets you login from many sources
	 * @param bOverride if false will fail if already logged in
	 * @return
	 */
/*	public static ObjectId createSession( ObjectId userid, boolean bMulti, boolean bOverride )
	{
		
		try
		{
			DBCollection cookieColl = DbManager.getSocial().getCookies();
			
			if (!bMulti) { // Otherwise allow multiple cookies for this user
				//remove any old cookie for this user
				BasicDBObject dbQuery = new BasicDBObject();
				dbQuery.put("profileId", userid);
				dbQuery.put("apiKey", new BasicDBObject(DbManager.exists_, false));
				DBCursor dbc = cookieColl.find(dbQuery);
				if (bOverride) {
					while (dbc.hasNext()) {
						cookieColl.remove(dbc.next());
					}
				}//TESTED
				else if (dbc.length() > 0) {
					return null;
				}//TESTED
			}
			//Find user
			//create a new entry
			CookiePojo cp = new CookiePojo();
			ObjectId randomObjectId = generateRandomId();
			
			cp.set_id(randomObjectId); 
			cp.setCookieId(randomObjectId);
			cp.setLastActivity(new Date());
			cp.setProfileId(userid);
			cp.setStartDate(new Date());
			cookieColl.insert(cp.toDb());
			//return cookieid
			return cp.getCookieId();
		}
		catch (Exception e )
		{
			logger.error("Line: [" + e.getStackTrace()[2].getLineNumber() + "] " + e.getMessage());
			e.printStackTrace();
		}
		
		return null;
	}
*/

}
