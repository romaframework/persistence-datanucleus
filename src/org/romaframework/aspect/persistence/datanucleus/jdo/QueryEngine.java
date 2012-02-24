package org.romaframework.aspect.persistence.datanucleus.jdo;

import java.util.List;

import javax.jdo.PersistenceManager;

import org.romaframework.aspect.persistence.QueryByExample;
import org.romaframework.aspect.persistence.QueryByFilter;
import org.romaframework.aspect.persistence.QueryByText;

public interface QueryEngine {

	public List<?> queryByExample(PersistenceManager manager, QueryByExample queryInput);

	public List<?> queryByFilter(PersistenceManager manager, QueryByFilter queryInput);

	public List<?> queryByText(PersistenceManager manager, QueryByText queryInput);

	public long countByExample(PersistenceManager manager, QueryByExample queryInput);

	public long countByFilter(PersistenceManager manager, QueryByFilter queryInput);

	public long countByText(PersistenceManager manager, QueryByText queryInput);
	
}
