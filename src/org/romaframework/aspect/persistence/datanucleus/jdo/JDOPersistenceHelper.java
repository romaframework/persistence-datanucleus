/*
 * Copyright 2006-2009 Luca Garulli (luca.garulli--at--assetdata.it)
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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.romaframework.aspect.persistence.QueryOperator;
import org.romaframework.aspect.persistence.PersistenceAspect;
import org.romaframework.aspect.persistence.PersistenceException;
import org.romaframework.aspect.persistence.QueryByExample;
import org.romaframework.aspect.persistence.QueryByFilter;
import org.romaframework.aspect.persistence.QueryByFilterItem;
import org.romaframework.aspect.persistence.QueryByFilterItemGroup;
import org.romaframework.aspect.persistence.QueryByFilterItemPredicate;
import org.romaframework.aspect.persistence.QueryByFilterItemText;
import org.romaframework.aspect.persistence.QueryByFilterOrder;
import org.romaframework.aspect.persistence.QueryByText;
import org.romaframework.core.Roma;
import org.romaframework.core.schema.SchemaClassDefinition;
import org.romaframework.core.schema.SchemaField;
import org.romaframework.core.schema.SchemaHelper;

/**
 * JDO Helper Class<?> that make all the work of translating and management of JDO resources.
 * 
 * @author Luca Garulli (luca.garulli--at--assetdata.it)
 * 
 */
public class JDOPersistenceHelper {

	public static final java.lang.String	SQL99_RESERVED_WORDS	= "ABSOLUTE,ACTION,ADD,AFTER,ALL,ALLOCATE,ALTER,AND,ANY,ARE,ARRAY,AS,ASC,ASENSITIVE,ASSERTION,ASYMMETRIC,AT,ATOMIC,AUTHORIZATION,BEFORE,BEGIN,BETWEEN,BINARY,BIT,BLOB,BOOLEAN,BOTH,BREADTH,BY,CALL,CALLED,CASCADE,CASCADED,CASE,CAST,CATALOG,CHAR,CHARACTER,CHECK,CLOB,CLOSE,COLLATE,COLLATION,COLUMN,COMMIT,CONDITION,CONNECT,CONNECTION,CONSTRAINT,CONSTRAINTS,CONSTRUCTOR,CONTINUE,CORRESPONDING,CREATE,CROSS,CUBE,CURRENT,CURRENT_DATE,CURRENT_DEFAULT_TRANSFORM_GROUP,CURRENT_PATH,CURRENT_ROLE,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_TRANSFORM_GROUP_FOR_TYPE,CURRENT_USER,CURSOR,CYCLE,DATA,DATE,DAY,DEALLOCATE,DEC,DECIMAL,DECLARE,DEFAULT,DEFERRABLE,DEFERRED,DELETE,DEPTH,DEREF,DESC,DESCRIBE,DESCRIPTOR,DETERMINISTIC,DIAGNOSTICS,DISCONNECT,DISTINCT,DO,DOMAIN,DOUBLE,DROP,DYNAMIC,EACH,ELSE,ELSEIF,END,EQUALS,ESCAPE,EXCEPT,EXCEPTION,EXEC,EXECUTE,EXISTS,EXIT,EXTERNAL,FALSE,FETCH,FILTER,FIRST,FLOAT,FOR,FOREIGN,FOUND,FREE,FROM,FULL,FUNCTION,GENERAL,GET,GLOBAL,GO,GOTO,GRANT,GROUP,GROUPING,HANDLER,HAVING,HOLD,HOUR,IDENTITY,IF,IMMEDIATE,IN,INDICATOR,INITIALLY,INNER,INOUT,INPUT,INSENSITIVE,INSERT,INT,INTEGER,INTERSECT,INTERVAL,INTO,IS,ISOLATION,ITERATE,JOIN,KEY,LANGUAGE,LARGE,LAST,LATERAL,LEADING,LEAVE,LEFT,LEVEL,LIKE,LOCAL,LOCALTIME,LOCALTIMESTAMP,LOCATOR,LOOP,MAP,MATCH,METHOD,MINUTE,MODIFIES,MODULE,MONTH,NAMES,NATIONAL,NATURAL,NCHAR,NCLOB,NEW,NEXT,NO,NONE,NOT,NULL,NUMERIC,OBJECT,OF,OLD,ON,ONLY,OPEN,OPTION,OR,ORDER,ORDINALITY,OUT,OUTER,OUTPUT,OVER,OVERLAPS,PAD,PARAMETER,PARTIAL,PARTITION,PATH,PRECISION,PREPARE,PRESERVE,PRIMARY,PRIOR,PRIVILEGES,PROCEDURE,PUBLIC,RANGE,READ,READS,REAL,RECURSIVE,REF,REFERENCES,REFERENCING,RELATIVE,RELEASE,REPEAT,RESIGNAL,RESTRICT,RESULT,RETURN,RETURNS,REVOKE,RIGHT,ROLE,ROLLBACK,ROLLUP,ROUTINE,ROW,ROWS,SAVEPOINT,SCHEMA,SCOPE,SCROLL,SEARCH,SECOND,SECTION,SELECT,SENSITIVE,SESSION,SESSION_USER,SET,SETS,SIGNAL,SIMILAR,SIZE,SMALLINT,SOME,SPACE,SPECIFIC,SPECIFICTYPE,SQL,SQLEXCEPTION,SQLSTATE,SQLWARNING,START,STATE,STATIC,SYMMETRIC,SYSTEM,SYSTEM_USER,TABLE,TEMPORARY,THEN,TIME,TIMESTAMP,TIMEZONE_HOUR,TIMEZONE_MINUTE,TO,TRAILING,TRANSACTION,TRANSLATION,TREAT,TRIGGER,TRUE,UNDER,UNDO,UNION,UNIQUE,UNKNOWN,UNNEST,UNTIL,UPDATE,USAGE,USER,USING,VALUE,VALUES,VARCHAR,VARYING,VIEW,WHEN,WHENEVER,WHERE,WHILE,WINDOW,WITH,WITHIN,WITHOUT,WORK,WRITE,YEAR,ZONE";

	private static final int							MODE_FIELD_THEN_PAR		= 0;
	private static final int							MODE_PAR_THEN_FIELD		= 1;
	private static Log										log										= LogFactory.getLog(JDOPersistenceHelper.class);
	private static final String						THIS_PREFIX_KEYWORD		= "this.";

	public static Query getQuery(PersistenceManager iManager, org.romaframework.aspect.persistence.Query iQuerySource, Class<?> iCandidateClass, int iRangeFrom,
			int iRangeTo, boolean iSubClasses, String iMode) {
		Query query = null;

		try {
			iManager.getFetchPlan().clearGroups();
			iManager.getFetchPlan().addGroup(PersistenceAspect.DEFAULT_MODE_LOADING);

			if (iQuerySource.getStrategy() != PersistenceAspect.STRATEGY_STANDARD && iMode != null)
				iManager.getFetchPlan().addGroup(iMode);

			if (iQuerySource instanceof QueryByText && ((QueryByText) iQuerySource).getCandidateClass() == null) {
				query = iManager.newQuery(((QueryByText) iQuerySource).getText());
			} else {
				query = iManager.newQuery();
				if (iCandidateClass != null) {
					if (iSubClasses)
						query.setClass(iCandidateClass);
					else
						query.setCandidates(iManager.getExtent(iCandidateClass, false));
				}
			}
		} catch (Throwable e) {
			log.error("[JDOPersistenceAspect.getQuery]", e);
			throw new PersistenceException("getQuery", e);
		}

		return query;
	}

	public static List<?> prepareQuery(PersistenceManager iManager, org.romaframework.aspect.persistence.Query iQuerySource, Query iQueryToExecute,
			Query iCountQuery, Map<String, Object> parValues) {
		try {
			// FIRST PAGE: GET RESULT NUMBER
			if (iQuerySource.getRangeFrom() == 0) {
				// BEFORE TO EXECUTE COUNT TOTAL ITEMS
				iCountQuery.setUnique(true);
				iCountQuery.setResult("count(this)");

				long totalItems = -1;

				if (parValues != null)
					totalItems = (Long) iCountQuery.executeWithMap(parValues);
				else
					totalItems = (Long) iCountQuery.execute();

				iQuerySource.setTotalItems((int) totalItems);

				iCountQuery.closeAll();
			}

			if (iQuerySource.getRangeFrom() > -1 && iQuerySource.getRangeTo() > -1)
				iQueryToExecute.setRange(iQuerySource.getRangeFrom(), iQuerySource.getRangeTo());

			// EXECUTE QUERY CONSIDERING PAGING
			List<?> result;
			if (parValues != null)
				result = (List<?>) iQueryToExecute.executeWithMap(parValues);
			else
				result = (List<?>) iQueryToExecute.execute();

			return result;
		} catch (Throwable e) {
			log.error("[JDOPersistenceAspect.prepareQuery]", e);
			throw new PersistenceException("prepareQuery", e);
		}
	}

	public static List<?> queryByExample(PersistenceManager manager, QueryByExample iQuery) throws PersistenceException {
		QueryByFilter filter = buildQueryByFilter(iQuery);

		List<?> result = queryByFilter(manager, filter);
		iQuery.setTotalItems(filter.getTotalItems());
		return result;
	}

	public static long countByExample(PersistenceManager manager, QueryByExample iQuery) throws PersistenceException {
		QueryByFilter filter = buildQueryByFilter(iQuery);
		return countByFilter(manager, filter);
	}

	private static QueryByFilter buildQueryByFilter(QueryByExample iQuery) {
		if (log.isDebugEnabled())
			log.debug("[JDOPersistenceAspect.queryByExample] Class: " + iQuery.getCandidateClass() + " filter object: " + iQuery);

		// TODO Use SchemaClassReflection to use method/field getters and setters

		// EXTRACT QUERY BASED ON FILER OBJECT
		QueryByFilter filter = new QueryByFilter(iQuery.getCandidateClass());
		filter.setRangeFrom(iQuery.getRangeFrom(), iQuery.getRangeTo());
		filter.setSubClasses(iQuery.isSubClasses());
		filter.setMode(iQuery.getMode());
		filter.setStrategy(iQuery.getStrategy());

		if (iQuery.getFilter() != null) {
			Field[] fields = SchemaHelper.getFields(iQuery.getCandidateClass());
			Object fieldValue;
			QueryOperator operator = null;
			for (int i = 0; i < fields.length; ++i) {
				try {
					if (Modifier.isStatic(fields[i].getModifiers()) || Modifier.isTransient(fields[i].getModifiers()))
						// JUMP STATIC AND TRANSIENT FIELDS
						continue;

					if (fields[i].getName().startsWith("jdo"))
						// IGNORE ALL JDO FIELDS
						continue;

					if (!fields[i].isAccessible())
						fields[i].setAccessible(true);

					fieldValue = fields[i].get(iQuery.getFilter());
					if (fieldValue != null) {
						if (fieldValue instanceof Collection<?> || fieldValue instanceof Map<?, ?>)
							continue;
						if (fieldValue instanceof String && ((String) fieldValue).length() == 0)
							// EMPTY STRING, IGNORE FOR FILTERING
							continue;

						if (fields[i].getType().equals(String.class))
							operator = QueryByFilter.FIELD_LIKE;
						else
							operator = QueryByFilter.FIELD_EQUALS;

						// INSERT INTO QUERY PREDICATE
						filter.addItem(fields[i].getName(), operator, fieldValue);
					}
				} catch (Exception e) {
					log.error("[JDOPersistenceAspect.queryByExample]", e);
				}
			}
		}

		QueryByFilter addFilter = iQuery.getAdditionalFilter();
		if (addFilter != null) {
			filter.setSubClasses(addFilter.isSubClasses());

			// COPY ALL ITEMS TO THE MAIN FILTER
			for (Iterator<QueryByFilterItem> it = addFilter.getItemsIterator(); it.hasNext();) {
				filter.addItem(it.next());
			}

			// COPY ALL ORDER CLAUSES TO THE MAIN FILTER
			for (Iterator<QueryByFilterOrder> it = addFilter.getOrdersIterator(); it.hasNext();) {
				filter.addOrder(it.next());
			}
		}
		return filter;
	}

	public static List<?> queryByFilter(PersistenceManager manager, QueryByFilter iQueryFilter) throws PersistenceException {
		if (iQueryFilter == null)
			return null;

		if (log.isDebugEnabled())
			log.debug("[JDOPersistenceAspect.queryByExample] Class: " + iQueryFilter.getCandidateClass() + " filter map: " + iQueryFilter);

		List<?> result = null;

		SchemaClassDefinition def = Roma.schema().getSchemaClass(iQueryFilter.getCandidateClass());

		// EXTRACT QUERY BASED ON FILER OBJECT
		StringBuilder queryText = new StringBuilder();
		StringBuilder queryParameters = new StringBuilder();
		StringBuilder queryVariables = new StringBuilder();
		StringBuilder queryOrders = new StringBuilder();

		HashMap<String, Object> parValues = new HashMap<String, Object>();

		formatParameters(iQueryFilter, def, queryText, queryParameters, queryVariables, parValues, null);

		Query query = getQuery(manager, iQueryFilter, iQueryFilter.getCandidateClass(), iQueryFilter.getRangeFrom(), iQueryFilter.getRangeTo(),
				iQueryFilter.isSubClasses(), iQueryFilter.getMode());
		Query countQuery = getQuery(manager, iQueryFilter, iQueryFilter.getCandidateClass(), iQueryFilter.getRangeFrom(), iQueryFilter.getRangeTo(),
				iQueryFilter.isSubClasses(), iQueryFilter.getMode());

		setOrdering(iQueryFilter, query, queryOrders);

		if (queryParameters.length() > 0) {
			query.declareParameters(queryParameters.toString());
			countQuery.declareParameters(queryParameters.toString());
		}

		if (queryVariables.length() > 0) {
			query.declareVariables(queryVariables.toString());
			countQuery.declareVariables(queryVariables.toString());
		}

		query.setFilter(queryText.toString());
		countQuery.setFilter(queryText.toString());

		List<?> tempResult = prepareQuery(manager, iQueryFilter, query, countQuery, parValues);

		if (iQueryFilter.getTotalItems() == -1)
			iQueryFilter.setTotalItems(tempResult != null ? tempResult.size() : 0);

		result = retrieveObjects(manager, iQueryFilter, tempResult);

		closeQuery(tempResult, query, iQueryFilter.getStrategy());

		return result;
	}

	public static long countByFilter(PersistenceManager manager, QueryByFilter iQueryFilter) {
		if (iQueryFilter == null)
			return 0;

		if (log.isDebugEnabled())
			log.debug("[JDOPersistenceAspect.queryByExample] Class: " + iQueryFilter.getCandidateClass() + " filter map: " + iQueryFilter);

		SchemaClassDefinition def = Roma.schema().getSchemaClass(iQueryFilter.getCandidateClass());

		// EXTRACT QUERY BASED ON FILER OBJECT
		StringBuilder queryText = new StringBuilder();
		StringBuilder queryParameters = new StringBuilder();
		StringBuilder queryVariables = new StringBuilder();

		HashMap<String, Object> parValues = new HashMap<String, Object>();

		formatParameters(iQueryFilter, def, queryText, queryParameters, queryVariables, parValues, null);

		Query countQuery = getQuery(manager, iQueryFilter, iQueryFilter.getCandidateClass(), iQueryFilter.getRangeFrom(), iQueryFilter.getRangeTo(),
				iQueryFilter.isSubClasses(), iQueryFilter.getMode());

		if (queryParameters.length() > 0) {
			countQuery.declareParameters(queryParameters.toString());
		}

		if (queryVariables.length() > 0) {
			countQuery.declareVariables(queryVariables.toString());
		}

		countQuery.setFilter(queryText.toString());

		long totalItems = 0;
		try {
			countQuery.setUnique(true);
			countQuery.setResult("count(this)");

			totalItems = (Long) countQuery.executeWithMap(parValues);

			countQuery.closeAll();
		} catch (Throwable e) {
			log.error("[JDOPersistenceAspect.prepareQuery]", e);
			throw new PersistenceException("prepareQuery", e);
		}

		return totalItems;
	}

	private static void closeQuery(List<?> result, Query query, byte iStrategy) {
		if (iStrategy == PersistenceAspect.STRATEGY_DETACHING || iStrategy == PersistenceAspect.STRATEGY_TRANSIENT)
			// CLOSE IMMEDIATELY THE QUERY TO SAVE RESOURCES
			query.close(result);
	}

	private static void formatItems(Iterator<QueryByFilterItem> items, SchemaClassDefinition def, StringBuilder queryText, StringBuilder queryParameters,
			StringBuilder queryVariables, HashMap<String, Object> parValues, String iObjectName, String predicateOperator) {
		Object itemValue;
		String parName;
		String tempParName;
		String typeName;
		QueryByFilterItem item;
		String fieldName;
		SchemaField field;
		boolean begin = true;

		for (Iterator<QueryByFilterItem> it = items; it.hasNext();) {
			item = it.next();

			if (item instanceof QueryByFilterItemText)
				queryText.append(((QueryByFilterItemText) item).getCondition());
			else if (item instanceof QueryByFilterItemGroup) {

				if (!begin) {
					queryText.append(translatePredicateOperatorBegin(predicateOperator));
				} else {
					begin = false;
				}
				QueryByFilterItemGroup group = (QueryByFilterItemGroup) item;
				Iterator<QueryByFilterItem> toIterate = group.getItems().iterator();
				queryText.append("(");
				formatItems(toIterate, def, queryText, queryParameters, queryVariables, parValues, iObjectName, group.getPredicate());
				queryText.append(")");
			} else if (item instanceof QueryByFilterItemPredicate) {
				QueryByFilterItemPredicate predicate = (QueryByFilterItemPredicate) item;
				parName = "par_" + predicate.getFieldName().replace('.', '_');
				if (parValues.containsKey(parName)) {
					tempParName = parName;
					int dupl = 0;

					while (parValues.containsKey(parName)) {
						parName = tempParName + (++dupl);
					}
				}

				fieldName = predicate.getFieldName();

				// SEARCH IF FIELD NAME IS A RESERVER KEYWORK
				String upperCaseFieldName = fieldName.toUpperCase();
				StringTokenizer tokenizer = new StringTokenizer(SQL99_RESERVED_WORDS, ",");
				while (tokenizer.hasMoreTokens()) {
					if (upperCaseFieldName.equals(tokenizer.nextToken())) {
						// PROTECT FIELD NAME BY PREFIXING 'THIS.'
						fieldName = THIS_PREFIX_KEYWORD + fieldName;
						break;
					}
				}

				field = def.getField(predicate.getFieldName());

				if (field == null)
					throw new PersistenceException("Cannot execute query: field " + predicate.getFieldName() + " not found in Class<?> " + def.getSchemaClass().getName());

				// GET FIELD VALUE
				itemValue = predicate.getFieldValue();

				switch (getOperatorMode(predicate.getFieldOperator())) {
				case MODE_FIELD_THEN_PAR:
					typeName = ((Class<?>) field.getLanguageType()).getName();
					break;
				case MODE_PAR_THEN_FIELD:
					typeName = itemValue.getClass().getName();
					break;
				default:
					return;
				}

				if (Collection.class.isAssignableFrom((Class<?>) field.getLanguageType()))
					// COLLECTION: GET EMBEDDED TYPE
					typeName = ((Class<?>) SchemaHelper.getEmbeddedType(itemValue, field).getLanguageType()).getName();

				// INSERT PREDICATE OPERATOR
				if (!begin) {
					queryText.append(translatePredicateOperatorBegin(predicateOperator));
				} else {
					begin = false;
				}

				// INSERT PARAMETER
				if (iObjectName != null) {
					queryText.append(iObjectName);
					queryText.append(".");
				}

				if (QueryByFilter.FIELD_NOT_IN.equals(predicate.getFieldOperator())) {
					queryText.append("!");
				}

				switch (getOperatorMode(predicate.getFieldOperator())) {
				case MODE_FIELD_THEN_PAR:
					queryText.append(fieldName);
					queryText.append(translateFieldOperatorBegin(predicate));
					queryText.append(parName);
					break;
				case MODE_PAR_THEN_FIELD:
					queryText.append(parName);
					queryText.append(translateFieldOperatorBegin(predicate));
					queryText.append(fieldName);
					break;

				default:
					break;
				}

				queryText.append(translateFieldOperatorEnd(predicate));

				queryText.append(translatePredicateOperatorEnd(predicateOperator));

				if (predicate.getFieldValue() instanceof QueryByFilter) {
					// NESTED FILTER: EXECUTE MYSELF RECURSIVELY
					Class<?> nestedClass = (Class<?>) field.getEmbeddedLanguageType();
					if (nestedClass == null)
						nestedClass = (Class<?>) SchemaHelper.getEmbeddedType(def.getField(fieldName));

					SchemaClassDefinition nestedDef = Roma.schema().getSchemaClass(nestedClass, null);

					// DECLARE VARIABLE
					if (queryVariables.length() > 0)
						queryVariables.append(", ");

					queryVariables.append(typeName);
					queryVariables.append(" ");
					queryVariables.append(parName);

					formatParameters((QueryByFilter) predicate.getFieldValue(), nestedDef, queryText, queryParameters, queryVariables, parValues, parName);
				} else {
					// DECLARE PARAMETER
					if (queryParameters.length() > 0)
						queryParameters.append(", ");

					queryParameters.append(typeName);
					queryParameters.append(" ");
					queryParameters.append(parName);

					if (predicate.getFieldOperator().equals(QueryByFilter.FIELD_LIKE))
						if (itemValue == null)
							itemValue = ".*.*";
						else {
							if (itemValue.toString().indexOf("*") != -1)
								itemValue = itemValue.toString().toUpperCase().replaceAll("[*]", ".*");
							else
								itemValue = ".*" + itemValue.toString().toUpperCase() + ".*";
						}

					parValues.put(parName, itemValue);
				}
			}
		}
	}

	private static void formatParameters(QueryByFilter iQueryFilter, SchemaClassDefinition def, StringBuilder queryText, StringBuilder queryParameters,
			StringBuilder queryVariables, HashMap<String, Object> parValues, String iObjectName) {
		formatItems(iQueryFilter.getItemsIterator(), def, queryText, queryParameters, queryVariables, parValues, iObjectName, iQueryFilter.getPredicateOperator());
	}

	public static void setOrdering(QueryByFilter iQueryFilter, Query query, StringBuilder queryOrders) {
		// SET ORDERING IF ANY
		QueryByFilterOrder order;
		for (Iterator<QueryByFilterOrder> it = iQueryFilter.getOrdersIterator(); it.hasNext();) {
			order = it.next();

			if (queryOrders.length() > 0)
				queryOrders.append(", ");

			queryOrders.append(order.getFieldName());
			queryOrders.append(translateOrderingMode(order.getFieldOrder()));
		}

		if (queryOrders.length() > 0)
			query.setOrdering(queryOrders.toString());
	}

	public static int getOperatorMode(QueryOperator iFieldOperator) {
		if (iFieldOperator.equals(QueryByFilter.FIELD_IN) || iFieldOperator.equals(QueryByFilter.FIELD_NOT_IN))
			return MODE_PAR_THEN_FIELD;

		return MODE_FIELD_THEN_PAR;
	}

	public static String translateFieldOperatorBegin(QueryByFilterItemPredicate iItem) {
		QueryOperator operator = iItem.getFieldOperator();

		if (operator.equals(QueryByFilter.FIELD_EQUALS))
			return " == ";
		else if (operator.equals(QueryByFilter.FIELD_MINOR))
			return " < ";
		else if (operator.equals(QueryByFilter.FIELD_MAJOR))
			return " > ";
		else if (operator.equals(QueryByFilter.FIELD_MINOR_EQUALS))
			return " <= ";
		else if (operator.equals(QueryByFilter.FIELD_MAJOR_EQUALS))
			return " >= ";
		else if (operator.equals(QueryByFilter.FIELD_NOT_EQUALS))
			return " != ";
		else if (operator.equals(QueryByFilter.FIELD_LIKE))
			return ".toUpperCase().matches(";
		else if (operator.equals(QueryByFilter.FIELD_CONTAINS))
			return ".contains(";
		else if (operator.equals(QueryByFilter.FIELD_IN))
			return ".contains(";
		else if (operator.equals(QueryByFilter.FIELD_NOT_IN))
			return ".contains(";

		return "";
	}

	public static String translateFieldOperatorEnd(QueryByFilterItemPredicate iItem) {
		QueryOperator operator = iItem.getFieldOperator();

		if (operator.equals(QueryByFilter.FIELD_LIKE))
			return ")";
		else if (operator.equals(QueryByFilter.FIELD_CONTAINS))
			return ")";
		else if (operator.equals(QueryByFilter.FIELD_IN))
			return ")";
		else if (operator.equals(QueryByFilter.FIELD_NOT_IN))
			return ")";
		return "";
	}

	public static String translatePredicateOperatorBegin(String iPredicateOperator) {
		if (iPredicateOperator.equals(QueryByFilter.PREDICATE_AND))
			return " && ";
		else if (iPredicateOperator.equals(QueryByFilter.PREDICATE_OR))
			return " || ";
		else if (iPredicateOperator.equals(QueryByFilter.PREDICATE_NOT))
			return " !";
		return "";
	}

	public static String translatePredicateOperatorEnd(String iPredicateOperator) {
		return "";
	}

	public static String translateOrderingMode(String iOrderMode) {
		if (iOrderMode.equals(QueryByFilter.ORDER_ASC))
			return " ascending";
		else if (iOrderMode.equals(QueryByFilter.ORDER_DESC))
			return " descending";
		return "";
	}

	public static long countByText(PersistenceManager manager, org.romaframework.aspect.persistence.QueryByText iQuerySource) throws PersistenceException {

		Class<?> iCandidateClass = iQuerySource.getCandidateClass();
		String iQuery = iQuerySource.getText();
		int iRangeFrom = iQuerySource.getRangeFrom();
		int iRangeTo = iQuerySource.getRangeTo();
		boolean iSubClasses = iQuerySource.isSubClasses();
		String iMode = iQuerySource.getMode();

		if (log.isDebugEnabled())
			log.debug("[JDOPersistenceAspect.query] " + iQuery);

		long result = 0;

		// EXECUTE QUERY
		Query countQuery = getQuery(manager, iQuerySource, iCandidateClass, iRangeFrom, iRangeTo, iSubClasses, iMode);

		if (!(iQuerySource instanceof QueryByText && ((QueryByText) iQuerySource).getCandidateClass() == null)) {
			countQuery.setFilter(iQuery);
		}

		Map<String, Object> params = iQuerySource.getParameters();

		try {
			// BEFORE TO EXECUTE COUNT TOTAL ITEMS
			countQuery.setUnique(true);
			countQuery.setResult("count(this)");

			result = (Long) countQuery.executeWithMap(params);

			countQuery.closeAll();

		} catch (Throwable e) {
			log.error("[JDOPersistenceAspect.prepareQuery]", e);
			throw new PersistenceException("prepareQuery", e);
		}
		return result;
	}

	public static List<?> queryByText(PersistenceManager manager, org.romaframework.aspect.persistence.QueryByText iQuerySource) throws PersistenceException {

		Class<?> iCandidateClass = iQuerySource.getCandidateClass();
		String iQuery = iQuerySource.getText();
		int iRangeFrom = iQuerySource.getRangeFrom();
		int iRangeTo = iQuerySource.getRangeTo();
		boolean iSubClasses = iQuerySource.isSubClasses();
		String iMode = iQuerySource.getMode();

		if (log.isDebugEnabled())
			log.debug("[JDOPersistenceAspect.query] " + iQuery);

		List<?> result = null;

		// EXECUTE QUERY
		Query query = getQuery(manager, iQuerySource, iCandidateClass, iRangeFrom, iRangeTo, iSubClasses, iMode);
		Query countQuery = getQuery(manager, iQuerySource, iCandidateClass, iRangeFrom, iRangeTo, iSubClasses, iMode);

		if (iQuerySource.getOrder() != null && iQuerySource.getOrder().length() > 0) {
			query.setOrdering(iQuerySource.getOrder());
		}

		if (!(iQuerySource instanceof QueryByText && ((QueryByText) iQuerySource).getCandidateClass() == null)) {
			query.setFilter(iQuery);
			countQuery.setFilter(iQuery);
		}

		Map<String, Object> params = iQuerySource.getParameters();

		prepareQuery(manager, iQuerySource, query, countQuery, params);

		List<?> tempResult = null;
		if (params == null) {
			tempResult = (List<?>) query.execute();
		} else {
			tempResult = (List<?>) query.executeWithMap(params);
		}
		result = retrieveObjects(manager, iQuerySource, tempResult);

		closeQuery(result, query, iQuerySource.getStrategy());

		if (iQuerySource.getTotalItems() == -1)
			iQuerySource.setTotalItems(result != null ? result.size() : 0);

		if (log.isDebugEnabled())
			log.debug("[JDOPersistenceAspect.query] Result: " + result.size());

		return result;
	}

	public static Object retrieveObject(PersistenceManager manager, String iMode, byte iStrategy, Object iObject) {
		if (iObject == null)
			return null;

		Object tempResult;

		switch (iStrategy) {
		case PersistenceAspect.STRATEGY_DETACHING:
			tempResult = manager.detachCopy(iObject);
			break;
		case PersistenceAspect.STRATEGY_TRANSIENT:
			tempResult = iObject;
			manager.makeTransient(tempResult, iMode != null);
			break;
		default:
			tempResult = iObject;
		}
		return tempResult;
	}

	public static List<?> retrieveObjects(PersistenceManager manager, org.romaframework.aspect.persistence.Query iQuerySource, List<?> tempResult) {
		switch (iQuerySource.getStrategy()) {
		case PersistenceAspect.STRATEGY_DETACHING:
			tempResult = (List<?>) manager.detachCopyAll(tempResult);
			break;
		case PersistenceAspect.STRATEGY_TRANSIENT:
			manager.makeTransientAll(tempResult, iQuerySource.getMode() != null);
			break;
		}
		return tempResult;
	}

	public static void closeManager(PersistenceManager manager) {
		if (manager != null && !manager.isClosed()) {
			if (manager.currentTransaction().isActive())
				manager.currentTransaction().rollback();

			manager.close();
		}
	}

	/**
	 * Get a configured JDO PersistenceManager instance from factory.
	 * 
	 * @return PersistenceManager from the factory
	 */
	public static PersistenceManager getPersistenceManager(PersistenceManagerFactory factory) {
		PersistenceManager pm = factory.getPersistenceManager();

		// SET NO LIMIT FOR FETCHING LINKED OBJECTS
		pm.getFetchPlan().setMaxFetchDepth(-1);

		return pm;
	}
}
