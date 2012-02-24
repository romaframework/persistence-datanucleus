package org.romaframework.aspect.persistence.datanucleus.jdo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.romaframework.aspect.persistence.PersistenceAspect;
import org.romaframework.aspect.persistence.QueryByExample;
import org.romaframework.aspect.persistence.QueryByFilter;
import org.romaframework.aspect.persistence.QueryByFilterItem;
import org.romaframework.aspect.persistence.QueryByFilterItemGroup;
import org.romaframework.aspect.persistence.QueryByFilterItemPredicate;
import org.romaframework.aspect.persistence.QueryByFilterItemText;
import org.romaframework.aspect.persistence.QueryByText;

public class JPQLQueryEngine implements QueryEngine {

	public Query createQuery(PersistenceManager manager, String query) {
		return manager.newQuery("javax.jdo.query.JPQL", query);
	}

	public long countByFilter(PersistenceManager manager, QueryByFilter queryInput) {
		return 0;
	}

	public long countByExample(PersistenceManager manager, QueryByExample queryInput) {
		// TODO Auto-generated method stub
		return 0;
	}

	public long countByText(PersistenceManager manager, QueryByText queryInput) {
		// TODO Auto-generated method stub
		return 0;
	}

	public List<?> queryByExample(PersistenceManager manager, QueryByExample queryInput) {
		// TODO Auto-generated method stub
		return null;
	}

	public List<?> queryByFilter(PersistenceManager manager, QueryByFilter queryInput) {

		return null;
	}

	public List<?> queryByText(PersistenceManager manager, QueryByText queryInput) {
		Query query = createQuery(manager, queryInput.getText());
		return executeQuery(manager, query, queryInput);
	}

	public List<?> executeQuery(PersistenceManager manager, Query query, org.romaframework.aspect.persistence.Query queryInput) {
		if (queryInput.getRangeFrom() > -1 && queryInput.getRangeTo() > -1)
			query.setRange(queryInput.getRangeFrom(), queryInput.getRangeTo());

		List<?> result = (List<?>) query.execute();

		byte strategy = queryInput.getStrategy();

		switch (strategy) {
		case PersistenceAspect.STRATEGY_DETACHING:
			result = (List<?>) manager.detachCopyAll(result);
			query.close(result);
			break;
		case PersistenceAspect.STRATEGY_TRANSIENT:
			manager.makeTransientAll(result, queryInput.getMode() != null);
			query.close(result);
			break;
		}

		return result;
	}

	public void buildQuery(QueryByFilter filter, StringBuilder query, Map<String, Object> params) {
		List<Class<?>> froms = new ArrayList<Class<?>>();
		froms.add(filter.getCandidateClass());
		if (filter.getItems() != null) {
			for (QueryByFilterItem item : filter.getItems()) {
				if (item instanceof QueryByFilterItemGroup) {

				} else if (item instanceof QueryByFilterItemPredicate) {

				}else if (item instanceof QueryByFilterItemText) {

				}else if (item instanceof QueryByFilterItemReverse) {

				}
			}
		}

		// query.append("select ");

	}

}
