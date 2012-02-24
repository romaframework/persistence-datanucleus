package org.romaframework.aspect.persistence.datanucleus.jdo;

import java.util.List;

import javax.jdo.PersistenceManager;

import org.romaframework.aspect.persistence.QueryByExample;
import org.romaframework.aspect.persistence.QueryByFilter;
import org.romaframework.aspect.persistence.QueryByText;

public class JDOPersistenceHelperQueryEngine implements QueryEngine {

	public List<?> queryByExample(PersistenceManager manager, QueryByExample queryInput) {
		return JDOPersistenceHelper.queryByExample(manager, queryInput);
	}

	public List<?> queryByFilter(PersistenceManager manager, QueryByFilter queryInput) {
		return JDOPersistenceHelper.queryByFilter(manager, queryInput);
	}

	public List<?> queryByText(PersistenceManager manager, QueryByText queryInput) {
		return JDOPersistenceHelper.queryByText(manager, queryInput);
	}

	public long countByExample(PersistenceManager manager, QueryByExample queryInput) {
		return JDOPersistenceHelper.countByExample(manager, queryInput);
	}

	public long countByFilter(PersistenceManager manager, QueryByFilter queryInput) {
		return JDOPersistenceHelper.countByFilter(manager, queryInput);
	}

	public long countByText(PersistenceManager manager, QueryByText queryInput) {
		return JDOPersistenceHelper.countByText(manager, queryInput);
	}

}
