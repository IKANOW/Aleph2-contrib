package com.ikanow.aleph2.security.module;

import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.session.mgt.SessionManager;
import com.google.inject.binder.AnnotatedBindingBuilder;

public class IkanowV1WebSecurityModule extends IkanowV1SecurityModule {

	@Override
	/**
	 * Partial functionality from ShiroModule. We do not want the whole guice functionality because the WebSecurityManager is created using the IniFactory.
	 */
	public void configure() {

        configureShiro();
        bind(realmCollectionKey())
                .to(realmSetKey());
        expose(realmCollectionKey());

    }

	@Override
    protected void bindSecurityManager(AnnotatedBindingBuilder<? super SecurityManager> bind) {
		// empty on purpose because we are using the WebSecurityManager created by the IniWebEnvironment
    }

    /**
     * Binds the session manager.  Override this method in order to provide your own session manager binding.
     * <p/>
     * By default, a {@link org.apache.shiro.session.mgt.DefaultSessionManager} is bound as an eager singleton.
     *
     * @param bind
     */
	@Override
    protected void bindSessionManager(AnnotatedBindingBuilder<SessionManager> bind) {
		// empty on purpose because we are using the WebSecurityManager created by the IniWebEnvironment
    }


}
