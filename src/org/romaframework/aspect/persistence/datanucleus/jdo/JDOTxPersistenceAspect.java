/*
 * Copyright 2006 Luca Garulli (luca.garulli--at--assetdata.it)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.romaframework.aspect.persistence.datanucleus.jdo;

import java.util.List;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.listener.InstanceLifecycleListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.romaframework.aspect.persistence.datanucleus.DataNucleusPersistenceModule;

/**
 * Class to handle persistence using DataNucleus tool and JDO 2 technology. Every method is not atomic, since it uses the same
 * transaction for all. Assure to call commit() or rollback() methods when finished to avoid resource consumption. JDO 2.0:
 * http://jcp.org/en/jsr/detail?id=243 <br/>
 * DataNucleus: http://www.datanucleus.org
 * 
 * @author Luca Garulli (luca.garulli--at--assetdata.it)
 */
public class JDOTxPersistenceAspect extends JDOBasePersistenceAspect {

	protected PersistenceManager	contextManager;
	private static Log						log	= LogFactory.getLog(JDOTxPersistenceAspect.class);

	public JDOTxPersistenceAspect(DataNucleusPersistenceModule iModule) {
		super(iModule);
	}

	public JDOTxPersistenceAspect(DataNucleusPersistenceModule iModule, Map<InstanceLifecycleListener, List<Class<?>>> listeners) {
		super(iModule,listeners);
	}

	public JDOTxPersistenceAspect(DataNucleusPersistenceModule iSource, byte iStrategy) {
		super(iSource, iStrategy);
	}

	@Override
	protected void init() {
		contextManager = JDOPersistenceHelper.getPersistenceManager(module.getPersistenceManagerFactory());

		if (contextManager.currentTransaction().isActive()) {
			// CLOSE ANY PREVIOUS TX IF ANY
			contextManager.currentTransaction().rollback();
		}

		if (listeners != null) {
			for (InstanceLifecycleListener l : listeners.keySet()) {
				contextManager.addInstanceLifecycleListener(l, listeners.get(l).toArray(new Class[listeners.size()]));
			}
		}

		contextManager.currentTransaction().begin();
		strategy = STRATEGY_DETACHING;
	}

	@Override
	public void setTxMode(byte txMode) {
		super.setTxMode(txMode);
		init();
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return contextManager;
	}

	@Override
	protected void beginOperation(PersistenceManager iManager) {
	}

	@Override
	protected void endOperation(PersistenceManager iManager) {
	}

	@Override
	protected void closeOperation(PersistenceManager iManager) {
	}

	public void commit() {
		log.debug("[JDOTxPersistenceAspect.commit]");

		contextManager.currentTransaction().commit();
		JDOPersistenceHelper.closeManager(contextManager);
	}

	public void rollback() {
		log.debug("[JDOTxPersistenceAspect.rollback]");

		contextManager.currentTransaction().rollback();
		JDOPersistenceHelper.closeManager(contextManager);
	}

	public void close() {
		if (contextManager.isClosed())
			return;

		if (contextManager.currentTransaction().isActive())
			rollback();
	}

	@Override
	protected void finalize() throws Throwable {
		JDOPersistenceHelper.closeManager(contextManager);
	}
}
