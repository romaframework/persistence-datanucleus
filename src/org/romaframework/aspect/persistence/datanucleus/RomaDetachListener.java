package org.romaframework.aspect.persistence.datanucleus;

import javax.jdo.PersistenceManager;
import javax.jdo.spi.Detachable;
import javax.jdo.spi.PersistenceCapable;

import org.datanucleus.ObjectManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.state.FetchPlanState;
import org.datanucleus.state.ObjectProviderFactory;
import org.datanucleus.state.StateManager;
import org.datanucleus.store.fieldmanager.DetachFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.util.DetachListener;
import org.romaframework.aspect.persistence.PersistenceAspect;
import org.romaframework.aspect.persistence.datanucleus.jdo.JDOAtomicPersistenceAspect;
import org.romaframework.core.Roma;

public class RomaDetachListener extends DetachListener {

	public RomaDetachListener() {
		DetachListener.setInstance(this);
	}

	@Override
	public void undetachedFieldAccess(Object entity, String field) {
		Roma.context().create();
		PersistenceAspect aspect = null;
		PersistenceManager manager = null;
		try {
			aspect = Roma.context().persistence();
			manager = (PersistenceManager) aspect.getUnderlyingComponent();
			if (manager instanceof JDOPersistenceManager && entity instanceof PersistenceCapable) {
				ObjectManager objManager = ((JDOPersistenceManager) manager).getObjectManager();
				ApiAdapter api = objManager.getApiAdapter();
				Object id = api.getIdForObject(entity);

				StateManager sm = (StateManager) ObjectProviderFactory.newForDetached(objManager, entity, id, api.getVersionForObject(entity));
				sm.retrieveDetachState(sm);
				AbstractClassMetaData acm = sm.getClassMetaData();
				try {
					int fieldPos = acm.getAbsolutePositionOfMember(field);
					sm.loadField(fieldPos);
					FetchPlanState fps = new FetchPlanState();
					FieldManager detachFieldManager = new DetachFieldManager(sm, acm.getSCOMutableMemberFlags(), objManager.getFetchPlan().manageFetchPlanForClass(acm), fps, false);
					detachFieldManager.fetchObjectField(fieldPos);
				} finally {
					((Detachable) entity).jdoReplaceDetachedState();
					((PersistenceCapable) entity).jdoReplaceStateManager(null);
				}
			}
		} finally {
			if (aspect == null || aspect instanceof JDOAtomicPersistenceAspect) {
				if (manager == null || manager.isClosed())
					return;
				if (manager.currentTransaction().isActive())
					manager.currentTransaction().rollback();
				manager.close();
				manager = null;
			}
			Roma.context().destroy();
		}
	}

}