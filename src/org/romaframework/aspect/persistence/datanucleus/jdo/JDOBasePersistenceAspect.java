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

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.listener.InstanceLifecycleListener;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.datanucleus.api.jdo.NucleusJDOHelper;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.romaframework.aspect.persistence.PersistenceAspect;
import org.romaframework.aspect.persistence.PersistenceAspectAbstract;
import org.romaframework.aspect.persistence.PersistenceException;
import org.romaframework.aspect.persistence.QueryByExample;
import org.romaframework.aspect.persistence.QueryByFilter;
import org.romaframework.aspect.persistence.QueryByText;
import org.romaframework.aspect.persistence.datanucleus.DataNucleusPersistenceModule;
import org.romaframework.aspect.persistence.jdbc.JDBCDatasource;
import org.romaframework.core.schema.SchemaField;

/**
 * Abstract class to handle persistence using DataNucleus tool and JDO 2 technology. JDO 2.0: http://jcp.org/en/jsr/detail?id=243 <br/>
 * DataNucleus: http://www.datanucleus.org.
 * 
 * @author Luca Garulli (luca.garulli--at--assetdata.it)
 */
@SuppressWarnings("unchecked")
public abstract class JDOBasePersistenceAspect extends PersistenceAspectAbstract implements JDBCDatasource {

	protected Map<InstanceLifecycleListener, List<Class<?>>> listeners;
	protected DataNucleusPersistenceModule	module;

	protected byte													strategy;
	protected byte													txMode	= TX_PESSIMISTIC;
	protected static Log										log			= LogFactory.getLog(JDOBasePersistenceAspect.class);

	public JDOBasePersistenceAspect(DataNucleusPersistenceModule iSource) {
		module = iSource;
		strategy = STRATEGY_DETACHING;
		init();
	}
	public JDOBasePersistenceAspect(DataNucleusPersistenceModule iSource,Map<InstanceLifecycleListener, List<Class<?>>> listeners) {
		module = iSource;
		strategy = STRATEGY_DETACHING;
		this.listeners = listeners;
		init();
	}
	public JDOBasePersistenceAspect(DataNucleusPersistenceModule iSource, byte iStrategy) {
		module = iSource;
		strategy = iStrategy;
		init();
	}

	protected abstract void init();

	protected abstract void beginOperation(PersistenceManager iManager);

	protected abstract void endOperation(PersistenceManager iManager);

	protected abstract void closeOperation(PersistenceManager iManager);

	public abstract PersistenceManager getPersistenceManager();

	/**
	 * Return the JDO Persistence Manager.
	 */
	public Object getUnderlyingComponent() {
		return getPersistenceManager();
	}

	public String getOID(Object iObject) throws PersistenceException {
		Object oid = JDOHelper.getObjectId(iObject);

		if (oid == null)
			return null;
		return oid.toString();
	}

	public <T> T loadObject(T iObject, String iMode) throws PersistenceException {
		return loadObject(iObject, iMode, strategy);
	}

	public <T> T loadObject(T iObject, String iMode, byte iStrategy) throws PersistenceException {
		if (log.isDebugEnabled())
			log.debug("[JDOPersistenceAspect.loadObject] obj: " + iObject + " mode: " + iMode + ", strategy: " + iStrategy);

		if (iStrategy == PersistenceAspect.STRATEGY_STANDARD && !JDOHelper.isDetached(iObject))
			return iObject;

		PersistenceManager manager = null;
		try {
			manager = getPersistenceManager();

			if (iMode != null)
				// SET FETCH GROUP POLICY
				manager.getFetchPlan().addGroup(iMode);

			beginOperation(manager);

			T freshObj = iObject;

			if (iStrategy == PersistenceAspect.STRATEGY_DETACHING || iStrategy == PersistenceAspect.STRATEGY_STANDARD) {
				Object oid = JDOHelper.getObjectId(iObject);
				if (oid == null)
					// NO OID, MAYBE EMBEDDED INSTANCE: JUST RETURN IT
					return null;

				freshObj = (T) manager.getObjectById(oid, true);
			}

			return (T) JDOPersistenceHelper.retrieveObject(manager, iMode, iStrategy, freshObj);
		} catch (Throwable e) {
			throw new PersistenceException("$PersistenceAspect.loadObject.error", e);
		} finally {
			endOperation(manager);
		}
	}

	public <T> T loadObjectByOID(String iOID, String iMode) throws PersistenceException {
		return (T) loadObjectByOID(iOID, iMode, strategy);
	}

	public <T> T loadObjectByOID(String iOID, String iMode, byte iStrategy) throws PersistenceException {
		if (iOID == null)
			return null;

		if (log.isDebugEnabled())
			log.debug("[JDOPersistenceAspect.loadObjectByOID] oid: " + iOID + " mode: " + iMode + ", strategy: " + iStrategy);

		PersistenceManager manager = null;
		try {
			manager = getPersistenceManager();

			if (iMode != null)
				// SET FETCH GROUP POLICY
				manager.getFetchPlan().addGroup(iMode);

			beginOperation(manager);

			T freshObj = null;

			Object oid = module.getOidManager().getOID(manager, iOID);

			freshObj = (T) manager.getObjectById(oid, true);

			return (T) JDOPersistenceHelper.retrieveObject(manager, iMode, iStrategy, freshObj);
		} catch (Throwable e) {
			throw new PersistenceException("$PersistenceAspect.loadObjectByOID.error", e);
		} finally {
			endOperation(manager);
		}
	}

	public <T> T createObject(Object iObject) throws PersistenceException {
		return (T) createObject(iObject, strategy);
	}

	public <T> T createObject(Object iObject, byte iStrategy) throws PersistenceException {
		if (iObject == null)
			return null;

		if (log.isDebugEnabled())
			log.debug("[JDOPersistenceAspect.createObject] " + iObject + " using strategy: " + iStrategy);

		PersistenceManager manager = null;
		try {
			manager = getPersistenceManager();
			beginOperation(manager);
			iObject = manager.makePersistent(iObject);

			return (T) JDOPersistenceHelper.retrieveObject(manager, null, iStrategy, iObject);
		} catch (Throwable e) {
			log.error("[JDOPersistenceAspect.createObject]", e);
			throw new PersistenceException("$PersistenceAspect.createObject.error", e);
		} finally {
			endOperation(manager);
		}
	}

	public <T> T updateObject(Object iObject) throws PersistenceException {
		return (T) updateObject(iObject, strategy);
	}

	public <T> T updateObject(Object iObject, byte iStrategy) throws PersistenceException {
		if (iObject == null)
			return null;

		PersistenceManager manager = null;
		try {
			manager = getPersistenceManager();
			beginOperation(manager);

			if (log.isDebugEnabled())
				log.debug("[JDOPersistenceAspect.updateObject] " + iObject + " using strategy: " + iStrategy);

			iObject = manager.makePersistent(iObject);
			
			return (T) JDOPersistenceHelper.retrieveObject(manager, null, iStrategy, iObject);
		} catch (Throwable e) {
			log.error("[JDOPersistenceAspect.updateObject]", e);
			throw new PersistenceException("$PersistenceAspect.updateObject.error", e);
		} finally {
			endOperation(manager);
		}
	}

	public Object[] updateObjects(Object[] iObjects) throws PersistenceException {
		return updateObjects(iObjects, strategy);
	}

	public Object[] updateObjects(Object[] iObjects, byte iStrategy) throws PersistenceException {
		if (iObjects == null)
			return null;

		PersistenceManager manager = null;
		Object[] returnObjects = new Object[iObjects.length];
		try {
			manager = getPersistenceManager();
			beginOperation(manager);

			Object currObject;
			for (int i = 0; i < iObjects.length; ++i) {
				currObject = iObjects[i];

				if (log.isDebugEnabled())
					log.debug("[JDOPersistenceAspect.updateObjects] " + currObject);

				currObject = manager.makePersistent(currObject);

				returnObjects[i] = JDOPersistenceHelper.retrieveObject(manager, null, iStrategy, currObject);
			}

			return returnObjects;
		} catch (Throwable e) {
			log.error("[JDOPersistenceAspect.updateObjects]", e);
			throw new PersistenceException("$PersistenceAspect.updateObject.error", e);
		} finally {
			endOperation(manager);
		}
	}

	public void deleteObjects(Object[] iObjects) throws PersistenceException {
		PersistenceManager manager = null;
		try {
			manager = getPersistenceManager();

			beginOperation(manager);

			Object currObject;
			for (int i = 0; i < iObjects.length; ++i) {
				currObject = iObjects[i];

				if (log.isDebugEnabled())
					log.debug("[JDOPersistenceAspect.deleteObjects] " + currObject);

				if (JDOHelper.isDetached(currObject)) {
					Object pObj = manager.makePersistent(currObject);
					manager.deletePersistent(pObj);
				} else {
					manager.deletePersistent(currObject);
				}
			}
		} catch (Throwable e) {
			log.error("[JDOPersistenceAspect.deleteObjects]", e);
			throw new PersistenceException("$PersistenceAspect.deleteObject.error", e);
		} finally {
			endOperation(manager);
		}
	}

	public void deleteObject(Object iObject) throws PersistenceException {
		PersistenceManager manager = null;
		try {
			manager = getPersistenceManager();

			beginOperation(manager);

			if (log.isDebugEnabled())
				log.debug("[JDOPersistenceAspect.deleteObject] " + iObject);

			if (JDOHelper.isDetached(iObject)) {
				Object pObj = manager.makePersistent(iObject);
				manager.deletePersistent(pObj);
			} else {
				manager.deletePersistent(iObject);
			}
		} catch (Throwable e) {
			log.error("[JDOPersistenceAspect.deleteObject]", e);
			throw new PersistenceException("$PersistenceAspect.deleteObject.error", e);
		} finally {
			endOperation(manager);
		}
	}

	public List<?> query(org.romaframework.aspect.persistence.Query iQuery) throws PersistenceException {
		PersistenceManager manager = null;
		List<?> result = null;
		try {
			manager = getPersistenceManager();

			beginOperation(manager);

			if (iQuery.getStrategy() == STRATEGY_DEFAULT)
				// ASSIGN PERSISTENCE ASPECT STRATEGY
				iQuery.setStrategy(strategy);

			if (iQuery instanceof QueryByExample) {
				QueryByExample queryInput = (QueryByExample) iQuery;
				result = JDOPersistenceHelper.queryByExample(manager, queryInput);
			} else if (iQuery instanceof QueryByFilter) {
				QueryByFilter queryInput = (QueryByFilter) iQuery;
				result = JDOPersistenceHelper.queryByFilter(manager, queryInput);
			} else if (iQuery instanceof QueryByText) {
				QueryByText queryInput = (QueryByText) iQuery;
				result = JDOPersistenceHelper.queryByText(manager, queryInput);
			}

			if (result != null)
				iQuery.setResult(result);

		} catch (Throwable e) {
			log.error("[JDOPersistenceAspect.query]", e);
			throw new PersistenceException("$PersistenceAspect.query.error", e);
		} finally {
			endOperation(manager);
		}
		return result;
	}

	public <T> T queryOne(org.romaframework.aspect.persistence.Query iQuery) throws PersistenceException {
		List<T> result = (List<T>) query(iQuery);
		if (result != null && result.size() > 0)
			return result.get(0);
		return null;
	}

	public boolean isObjectLocallyModified(Object iObject) throws PersistenceException {
		return JDOHelper.isDirty(iObject);
	}

	public boolean isObjectPersistent(Object iObject) throws PersistenceException {
		return JDOHelper.isPersistent(iObject) || JDOHelper.isDetached(iObject);
	}

	public boolean isClassPersistent(Class<?> iClass) {
		ClassMetaData classMetaData = NucleusJDOHelper.getMetaDataForClass(module.getPersistenceManagerFactory(), iClass);
		if (classMetaData != null) {
			ClassPersistenceModifier modifier = classMetaData.getPersistenceModifier();
			if (modifier != null)
				return modifier.equals(ClassPersistenceModifier.PERSISTENCE_CAPABLE)
						|| modifier.equals(ClassPersistenceModifier.PERSISTENCE_AWARE);
		}
		return false;
	}

	public boolean isFieldPersistent(Class<?> iClass, String iFieldName) {
		ClassMetaData classMetaData = NucleusJDOHelper.getMetaDataForClass(module.getPersistenceManagerFactory(), iClass);
		if (classMetaData != null) {
			AbstractMemberMetaData fieldMetaData = classMetaData.getMetaDataForMember(iFieldName);
			if (fieldMetaData != null) {
				FieldPersistenceModifier modifier = fieldMetaData.getPersistenceModifier();
				if (modifier != null)
					return modifier.equals(FieldPersistenceModifier.PERSISTENT) || modifier.equals(FieldPersistenceModifier.DEFAULT);
			}
		}
		return false;
	}

	public boolean isFieldPersistent(SchemaField iField) {
		if (iField != null) {
			return isFieldPersistent((Class<?>) iField.getEntity().getSchemaClass().getLanguageType(), iField.getName());
		}
		return false;
	}

	public void setObjectDirty(Object object, String fieldName) {
		JDOHelper.makeDirty(object, fieldName);
	}

	public Map<InstanceLifecycleListener, List<Class<?>>> getListeners() {
		return listeners;
	}
	public void setListeners(Map<InstanceLifecycleListener, List<Class<?>>> listeners) {
		this.listeners = listeners;
	}
	public byte getStrategy() {
		return strategy;
	}
	protected void setStrategy(byte iStrategy) {
		this.strategy = iStrategy;
	}
	
	public Connection getConnection() {
		return (Connection) JDOPersistenceHelper.getPersistenceManager(module.getPersistenceManagerFactory()).getDataStoreConnection()
				.getNativeConnection();
	}

	public boolean isActive() {
		return !getPersistenceManager().isClosed();
	}

	public byte getTxMode() {
		return txMode;
	}

	public void setTxMode(byte txMode) {
		this.txMode = txMode;
	}
}
