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
 * Class to handle persistence using DataNucleus tool and JDO 2 technology. Every method is atomic, since it uses a different JDO
 * PersistenceManager for all. Assure to call commit() or rollback() methods when finished to avoid resource consumption. JDO 2.0:
 * http://jcp.org/en/jsr/detail?id=243 <br/>
 * DataNucleus: http://www.datanucleus.org
 * 
 * @author Luca Garulli (luca.garulli--at--assetdata.it)
 */
public class JDOAtomicPersistenceAspect extends JDOBasePersistenceAspect {

	public JDOAtomicPersistenceAspect(DataNucleusPersistenceModule iModule) {
		super(iModule);
	}

	public JDOAtomicPersistenceAspect(DataNucleusPersistenceModule iSource, byte iStrategy) {
		super(iSource, iStrategy);
	}

	@Override
	protected void init() {
		strategy = STRATEGY_DETACHING;
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		PersistenceManager pm = JDOPersistenceHelper.getPersistenceManager(module.getPersistenceManagerFactory());
		pm.setDetachAllOnCommit(true);
		return pm;
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
		JDOPersistenceHelper.closeManager(iManager);
	}

	public void commit() {
	}

	public void rollback() {
	}

	public void close() {
	}
}
