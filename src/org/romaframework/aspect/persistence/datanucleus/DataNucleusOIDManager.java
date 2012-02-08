package org.romaframework.aspect.persistence.datanucleus;

import javax.jdo.PersistenceManager;

import org.romaframework.aspect.persistence.datanucleus.jdo.OIDManager;

public class DataNucleusOIDManager implements OIDManager {

	public Object getOID(PersistenceManager manager, String iOid) {
		return new org.datanucleus.identity.OIDImpl(iOid);
	}
}
