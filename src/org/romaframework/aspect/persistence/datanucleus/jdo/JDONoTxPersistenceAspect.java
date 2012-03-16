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

import javax.jdo.PersistenceManager;

import org.romaframework.aspect.persistence.datanucleus.DataNucleusPersistenceModule;

/**
 * Class to handle persistence using DataNucleus tool and JDO 2 technology. Every method is not atomic, since it uses the same JDO
 * PersistenceManager for all. Assure to call commit() or rollback() methods when finished to avoid resource consumption. JDO 2.0:
 * http://jcp.org/en/jsr/detail?id=243 <br/>
 * DataNucleus: http://www.datanucleus.org
 * 
 * @author Luca Garulli (luca.garulli--at--assetdata.it)
 */
public class JDONoTxPersistenceAspect extends JDOBasePersistenceAspect {

	protected PersistenceManager	contextManager;

	public JDONoTxPersistenceAspect(DataNucleusPersistenceModule iModule) {
		super(iModule);
	}

	public JDONoTxPersistenceAspect(DataNucleusPersistenceModule iSource, byte iStrategy) {
		super(iSource, iStrategy);
	}

	@Override
	protected void init() {
		if (contextManager != null)
			closeManager(contextManager);
		contextManager = createManager();

		strategy = STRATEGY_DETACHING;
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return contextManager;
	}

	@Override
	protected void beginOperation(PersistenceManager iManager) {
		iManager.currentTransaction().begin();
	}

	@Override
	protected void endOperation(PersistenceManager iManager) {
		iManager.currentTransaction().commit();
	}

	@Override
	protected void closeOperation(PersistenceManager iManager) {
	}

	public void commit() {
		close();
	}

	public void rollback() {
		close();
	}

	public void close() {
		closeManager(contextManager);
		contextManager = null;
	}

	@Override
	protected void finalize() throws Throwable {
		close();
	}
}
