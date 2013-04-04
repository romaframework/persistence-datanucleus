package org.romaframework.aspect.persistence.datanucleus;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.romaframework.aspect.persistence.PersistenceAspect;
import org.romaframework.aspect.persistence.PersistenceContextInjector;
import org.romaframework.aspect.persistence.datanucleus.jdo.OIDManager;
import org.romaframework.core.Roma;
import org.romaframework.core.aspect.AspectManager;
import org.romaframework.core.config.RomaApplicationContext;
import org.romaframework.core.module.SelfRegistrantConfigurableModule;

public class DataNucleusPersistenceModule extends SelfRegistrantConfigurableModule<String> {
	protected PersistenceManagerFactory								persistenceManagerFactory;
	protected OIDManager															oidManager;
	protected static final PersistenceContextInjector	injector	= new PersistenceContextInjector();

	protected static Log															log				= LogFactory.getLog(DataNucleusPersistenceModule.class);

	public DataNucleusPersistenceModule(OIDManager iOIDManager) {
		new RomaDetachListener();
		oidManager = iOIDManager;
	}

	@Override
	public void startup() throws RuntimeException {
		init();
	}

	@Override
	public void shutdown() throws RuntimeException {
		if (persistenceManagerFactory != null)
			try {
				persistenceManagerFactory.close();
			} catch (Exception e) {
			}
	}

	/**
	 * Assure, at the last chance, to close the factory.
	 */
	@Override
	protected void finalize() throws Throwable {
		shutdown();
	}

	@Deprecated
	public boolean isRuntimeEnhancement() {
		return false;
	}

	/**
	 * Activate the runtime enhancer if runtimeEnhancement is true.
	 * 
	 * @param runtimeEnhancement
	 */
	@Deprecated
	public void setRuntimeEnhancement(boolean runtimeEnhancement) {

	}

	public PersistenceManagerFactory getPersistenceManagerFactory() {
		init();
		return persistenceManagerFactory;
	}

	public OIDManager getOidManager() {
		return oidManager;
	}

	public static PersistenceContextInjector getInjector() {
		return injector;
	}

	@Override
	public String moduleName() {
		return "DataNucleus persistence";
	}

	private void init() {
		if (persistenceManagerFactory == null)
			synchronized (this) {
				if (persistenceManagerFactory == null) {
					persistenceManagerFactory = JDOHelper.getPersistenceManagerFactory(configuration);
				}
			}

	}
}
