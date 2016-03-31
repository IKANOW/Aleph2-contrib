package com.ikanow.aleph2.security.shiro;

import java.io.Serializable;

import org.apache.shiro.session.Session;
import org.apache.shiro.session.mgt.ValidatingSession;
import org.apache.shiro.session.mgt.eis.CachingSessionDAO;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.security.db.SessionDb;

/**
 * This class stores the session in the mongodb database
 * @author jfreydank
 *
 */
public class  MongoDbSessionDao extends CachingSessionDAO {

	protected final IServiceContext _context;
	protected SessionDb sessionDb;

	
	@Inject
	public MongoDbSessionDao(final IServiceContext service_context){
		super();
		_context = service_context;
		this.sessionDb = new SessionDb(service_context);
	}
	
    @Override
    protected Serializable doCreate(Session session) {
        Serializable sessionId = generateSessionId(session);
        assignSessionId(session, sessionId);
        sessionDb.store(session);
        return session.getId();
    }
    @Override
    protected void doUpdate(Session session) {
        if(session instanceof ValidatingSession && !((ValidatingSession)session).isValid()) {
            return; 
        }
        sessionDb.store(session);
    }
    @Override
    protected void doDelete(Session session) {
        sessionDb.delete(session.getId().toString());
    }
    @Override
    protected Session doReadSession(Serializable sessionId) {
        Session session = (Session)sessionDb.loadById(sessionId.toString());
        return session;
    }
}
