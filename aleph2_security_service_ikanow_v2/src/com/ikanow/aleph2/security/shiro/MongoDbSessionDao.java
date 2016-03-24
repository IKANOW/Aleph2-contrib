package com.ikanow.aleph2.security.shiro;

import org.apache.shiro.session.Session;
import org.apache.shiro.session.UnknownSessionException;
import org.apache.shiro.session.mgt.ValidatingSession;
import org.apache.shiro.session.mgt.eis.CachingSessionDAO;
import org.apache.shiro.session.mgt.eis.SessionDAO;

import com.google.inject.Inject;
import com.ikanow.aleph2.data_model.interfaces.shared_services.IServiceContext;
import com.ikanow.aleph2.security.db.SessionDb;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

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
        String sql = "insert into sessions(id, session) values(?,?)";
        //TODO
        //jdbcTemplate.update(sql, sessionId, SerializableUtils.serialize(session));
        
        sessionDb.store(session);
        return session.getId();
    }
    @Override
    protected void doUpdate(Session session) {
        if(session instanceof ValidatingSession && !((ValidatingSession)session).isValid()) {
            return; 
        }
        //String sql = "update sessions set session=? where id=?";        
        //jdbcTemplate.update(sql, SerializableUtils.serialize(session), session.getId());
        //TODO
        sessionDb.store(session);
    }
    @Override
    protected void doDelete(Session session) {
        String sql = "delete from sessions where id=?";
        //jdbcTemplate.update(sql, session.getId());
        sessionDb.delete(session.getId());
    }
    @Override
    protected Session doReadSession(Serializable sessionId) {
        String sql = "select session from sessions where id=?";
        //List<String> sessionStrList = jdbcTemplate.queryForList(sql, String.class, sessionId);
        //if(sessionStrList.size() == 0) 
        	//{return null; }
        //return SerializableUtils.deserialize(sessionStrList.get(0));
        Session session = sessionDb.load(sessionId);
        return null;
    }
}
