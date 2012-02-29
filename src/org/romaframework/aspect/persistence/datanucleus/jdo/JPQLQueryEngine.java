package org.romaframework.aspect.persistence.datanucleus.jdo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.romaframework.aspect.persistence.PersistenceAspect;
import org.romaframework.aspect.persistence.PersistenceException;
import org.romaframework.aspect.persistence.QueryByExample;
import org.romaframework.aspect.persistence.QueryByFilter;
import org.romaframework.aspect.persistence.QueryByFilterItem;
import org.romaframework.aspect.persistence.QueryByFilterItemGroup;
import org.romaframework.aspect.persistence.QueryByFilterItemPredicate;
import org.romaframework.aspect.persistence.QueryByFilterItemReverse;
import org.romaframework.aspect.persistence.QueryByFilterItemText;
import org.romaframework.aspect.persistence.QueryByFilterProjection;
import org.romaframework.aspect.persistence.QueryByFilterProjection.ProjectionOperator;
import org.romaframework.aspect.persistence.QueryByText;
import org.romaframework.aspect.persistence.QueryOperator;
import org.romaframework.core.Roma;
import org.romaframework.core.schema.SchemaField;

public class JPQLQueryEngine implements QueryEngine {

	protected static Log	log	= LogFactory.getLog(JPQLQueryEngine.class);

	public Query createQuery(PersistenceManager manager, String query) {
		if (log.isDebugEnabled())
			log.debug("Executing query:" + query);
		return manager.newQuery("javax.jdo.query.JPQL", query);
	}

	public long countByFilter(PersistenceManager manager, QueryByFilter queryInput) {
		queryInput.getProjections().clear();
		queryInput.addProjection("*", ProjectionOperator.COUNT);
		Object o = queryByFilter(manager, queryInput).get(0);
		if (o instanceof Number) {
			return ((Number) o).longValue();
		}
		throw new PersistenceException("Error no execute count , result not a number");
	}

	public long countByExample(PersistenceManager manager, QueryByExample queryInput) {
		return countByFilter(manager, buildQueryByFilter(queryInput));
	}

	public long countByText(PersistenceManager manager, QueryByText queryInput) {
		return 0;
	}

	public List<?> queryByExample(PersistenceManager manager, QueryByExample queryInput) {
		return queryByFilter(manager, buildQueryByFilter(queryInput));
	}

	public List<?> queryByFilter(PersistenceManager manager, QueryByFilter queryInput) {
		StringBuilder stringQuery = new StringBuilder();
		Map<String, Object> params = new HashMap<String, Object>();
		buildQuery(queryInput, stringQuery, params);
		Query query = createQuery(manager, stringQuery.toString());
		return executeQuery(manager, query, queryInput, params);
	}

	public List<?> queryByText(PersistenceManager manager, QueryByText queryInput) {
		Query query = createQuery(manager, queryInput.getText());
		return executeQuery(manager, query, queryInput, null);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<?> executeQuery(PersistenceManager manager, Query query, org.romaframework.aspect.persistence.Query queryInput, Map<String, Object> params) {
		if (queryInput.getRangeFrom() > -1 && queryInput.getRangeTo() > -1)
			query.setRange(queryInput.getRangeFrom(), queryInput.getRangeTo());
		Object result;
		if (params == null || params.isEmpty())
			result = query.execute();
		else
			result = query.executeWithMap(params);

		if (result instanceof Collection<?>) {
			byte strategy = queryInput.getStrategy();
			switch (strategy) {
			case PersistenceAspect.STRATEGY_DETACHING:
				result = manager.detachCopyAll((Collection<?>) result);
				query.close(result);
				break;
			case PersistenceAspect.STRATEGY_TRANSIENT:
				manager.makeTransientAll(result, queryInput.getMode() != null);
				query.close(result);
				break;
			}
		}

		if (!(result instanceof List)) {
			if (result instanceof Set<?>) {
				result = new ArrayList((Set<?>) result);
			} else
				result = Arrays.asList(result);
		}

		return (List<?>) result;
	}

	public void buildQuery(QueryByFilter filter, StringBuilder query, Map<String, Object> params) {
		StringBuilder where = new StringBuilder();
		Map<Character, Class<?>> froms = new HashMap<Character, Class<?>>();
		if (!filter.getItems().isEmpty()) {
			where.append(" where ");
			buildWhere(where, filter.getItems(), params, filter.getPredicateOperator(), 'A', froms);
		}
		query.append("select ");
		StringBuilder groupBy = new StringBuilder();
		if (!filter.getProjections().isEmpty()) {
			buildProjection(query, groupBy, filter.getProjections(), 'A');
		} else {
			query.append('A');
		}
		query.append(" from ").append(filter.getCandidateClass().getName()).append(" A");
		for (Map.Entry<Character, Class<?>> from : froms.entrySet()) {
			query.append(',').append(from.getValue().getName()).append(' ').append(from.getKey());
		}
		query.append(where);
		query.append(groupBy);
	}

	public void buildProjection(StringBuilder pro, StringBuilder groupBy, List<QueryByFilterProjection> projections, char alias) {
		if (projections != null) {
			Iterator<QueryByFilterProjection> projectionI = projections.iterator();
			boolean hasFunction = false, hasSimple = false;
			while (projectionI.hasNext()) {
				QueryByFilterProjection projection = projectionI.next();
				if (!ProjectionOperator.PLAIN.equals(projection.getOperator()))
					hasFunction = true;
				else
					hasSimple = true;
				resolveProjection(pro, projection.getField(), projection.getOperator(), alias);
				if (projectionI.hasNext())
					pro.append(',');
			}
			projectionI = projections.iterator();
			if (hasSimple && hasFunction) {
				groupBy.append(" group by ");
				boolean needSep = false;
				while (projectionI.hasNext()) {
					QueryByFilterProjection projection = projectionI.next();
					if (ProjectionOperator.PLAIN.equals(projection.getOperator())) {
						if (needSep)
							groupBy.append(',');
						else
							needSep = true;
						resolveProjection(groupBy, projection.getField(), projection.getOperator(), alias);
					}
				}
			}
		}
	}

	private void resolveProjection(StringBuilder build, String field, ProjectionOperator oper, char alias) {
		switch (oper) {
		case AVG:
			build.append("AVG(").append(alias).append('.').append(field).append(')');
			break;
		case COUNT:
			if ("*".equals(field))
				build.append("COUNT(").append(alias).append(')');
			else
				build.append("COUNT(").append(alias).append('.').append(field).append(')');
			break;
		case MAX:
			build.append("MAX(").append(alias).append('.').append(field).append(')');
			break;
		case MIN:
			build.append("MIN(").append(alias).append('.').append(field).append(')');
			break;
		case SUM:
			build.append("SUM(").append(alias).append('.').append(field).append(')');
			break;
		case PLAIN:
			build.append(alias).append('.').append(field);
			break;
		}
	}

	public void buildWhere(StringBuilder where, List<QueryByFilterItem> items, Map<String, Object> params, String predicate, char alias, Map<Character, Class<?>> froms) {
		if (items == null)
			return;
		Iterator<QueryByFilterItem> iter = items.iterator();
		while (iter.hasNext()) {
			QueryByFilterItem item = iter.next();
			if (item instanceof QueryByFilterItemGroup) {
				if (((QueryByFilterItemGroup) item).getItems() == null && ((QueryByFilterItemGroup) item).getItems().isEmpty())
					continue;
				where.append("(");
				buildWhere(where, ((QueryByFilterItemGroup) item).getItems(), params, ((QueryByFilterItemGroup) item).getPredicate(), alias, froms);
				where.append(")");
			} else if (item instanceof QueryByFilterItemPredicate) {
				QueryByFilterItemPredicate pred = ((QueryByFilterItemPredicate) item);
				String fieldName = pred.getFieldName();
				where.append(alias).append(".").append(fieldName);
				where.append(getJPQLOperator(pred.getFieldOperator()));
				String pName = fieldName.replace('.', '_');
				int i = 1;
				while (params.get(pName) != null)
					pName = fieldName.replace('.', '_') + (i++);
				if (QueryOperator.LIKE.equals(pred.getFieldOperator()) && (pred.getFieldValue() instanceof String) || pred.getFieldValue() == null) {
					String value = (String) pred.getFieldValue();
					if (value == null)
						params.put(pName, "%");
					else {
						if (value.indexOf("*") != -1)
							params.put(pName, value.toUpperCase().replaceAll("[*]", "%"));
						else
							params.put(pName, "%" + value.toUpperCase() + "%");
					}
				} else
					params.put(pName, pred.getFieldValue());
				where.append(':').append(pName);
			} else if (item instanceof QueryByFilterItemText) {
				where.append(alias).append(".").append(((QueryByFilterItemText) item).getCondition());
			} else if (item instanceof QueryByFilterItemReverse) {
				String field = ((QueryByFilterItemReverse) item).getField();
				char newAlias = alias;
				newAlias++;
				where.append(newAlias).append(".").append(field);
				where.append(getJPQLOperator(((QueryByFilterItemReverse) item).getOperator()));
				where.append(alias);
				QueryByFilter qbf = ((QueryByFilterItemReverse) item).getQueryByFilter();
				froms.put(newAlias, qbf.getCandidateClass());
				if (qbf.getItems().size() > 0) {
					where.append(" ").append(predicate).append(" ");
				}
				buildWhere(where, qbf.getItems(), params, qbf.getPredicateOperator(), newAlias, froms);
			}
			if (iter.hasNext())
				where.append(" ").append(predicate).append(" ");
		}
	}

	private String getJPQLOperator(QueryOperator operator) {
		switch (operator) {
		case CONTAINS:
		case IN:
			return " IN ";
		case LIKE:
			return " LIKE ";
		case EQUALS:
			return " = ";
		case MAJOR_EQUALS:
			return " >= ";
		case MAJOR:
			return " > ";
		case MINOR:
			return " < ";
		case MINOR_EQUALS:
			return " <= ";
		case NOT_EQUALS:
			return " <> ";
		case NOT_IN:
			return " NOT IN ";
		}
		return "";
	}

	protected QueryByFilter buildQueryByFilter(QueryByExample iQuery) {

		QueryByFilter filter = new QueryByFilter(iQuery.getCandidateClass());
		filter.setRangeFrom(iQuery.getRangeFrom(), iQuery.getRangeTo());
		filter.setSubClasses(iQuery.isSubClasses());
		filter.setMode(iQuery.getMode());
		filter.setStrategy(iQuery.getStrategy());

		if (iQuery.getFilter() != null) {
			Iterator<SchemaField> sf = Roma.schema().getSchemaClass(iQuery.getCandidateClass()).getFieldIterator();
			Object fieldValue;
			QueryOperator operator = null;
			while (sf.hasNext()) {
				SchemaField field = sf.next();
				fieldValue = field.getValue(iQuery.getFilter());
				if (fieldValue != null) {
					if (fieldValue instanceof Collection<?> || fieldValue instanceof Map<?, ?>)
						continue;
					if (fieldValue instanceof String && ((String) fieldValue).length() == 0)
						continue;

					if (String.class.equals(field.getLanguageType()))
						operator = QueryByFilter.FIELD_LIKE;
					else
						operator = QueryByFilter.FIELD_EQUALS;

					filter.addItem(field.getName(), operator, fieldValue);
				}
			}
		}

		QueryByFilter addFilter = iQuery.getAdditionalFilter();
		if (addFilter != null) {
			filter.setSubClasses(addFilter.isSubClasses());
			filter.merge(addFilter);
		}
		return filter;
	}

}
