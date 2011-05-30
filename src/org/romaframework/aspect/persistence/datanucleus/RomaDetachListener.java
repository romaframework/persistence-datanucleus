package org.romaframework.aspect.persistence.datanucleus;

import javax.jdo.PersistenceManager;
import javax.jdo.spi.Detachable;
import javax.jdo.spi.PersistenceCapable;

import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.jdo.JDOPersistenceManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.state.FetchPlanState;
import org.datanucleus.state.StateManagerFactory;
import org.datanucleus.store.fieldmanager.DetachFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.util.DetachListener;
import org.romaframework.aspect.persistence.PersistenceAspect;
import org.romaframework.core.Roma;

public class RomaDetachListener extends DetachListener {

	public RomaDetachListener() {
		DetachListener.setInstance(this);
	}

	@Override
	public void undetachedFieldAccess(Object entity, String field) {
		try {
			Roma.context().create();
			PersistenceAspect aspect = Roma.context().persistence();
			PersistenceManager manager = (PersistenceManager) aspect.getUnderlyingComponent();
			if (manager instanceof JDOPersistenceManager && entity instanceof PersistenceCapable) {
				ObjectManager objManager = ((JDOPersistenceManager) manager).getObjectManager();
				ApiAdapter api = objManager.getApiAdapter();
				Object id = api.getIdForObject(entity);

				StateManager sm = StateManagerFactory.newStateManagerForDetached(objManager.getExecutionContext(), entity, id,
						api.getVersionForObject(entity));
				sm.retrieveDetachState(sm);
				AbstractClassMetaData acm = sm.getClassMetaData();
				int fieldPos = acm.getAbsolutePositionOfMember(field);
				sm.loadField(fieldPos);
				FetchPlanState fps = new FetchPlanState();
				FieldManager detachFieldManager = new DetachFieldManager(sm.getObjectProvider(), acm.getSCOMutableMemberFlags(), objManager
						.getFetchPlan().manageFetchPlanForClass(acm), fps, false);
				detachFieldManager.fetchObjectField(fieldPos);
				((Detachable) entity).jdoReplaceDetachedState();
				((PersistenceCapable) entity).jdoReplaceStateManager(null);
			}
		} finally {
			Roma.context().destroy();
		}
	}

}