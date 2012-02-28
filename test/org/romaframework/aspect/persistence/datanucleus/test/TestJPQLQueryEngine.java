package org.romaframework.aspect.persistence.datanucleus.test;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.romaframework.aspect.persistence.QueryByFilter;
import org.romaframework.aspect.persistence.QueryByFilterItemGroup;
import org.romaframework.aspect.persistence.QueryByFilterProjection.ProjectionOperator;
import org.romaframework.aspect.persistence.QueryOperator;
import org.romaframework.aspect.persistence.datanucleus.jdo.JPQLQueryEngine;
import org.romaframework.core.Roma;

public class TestJPQLQueryEngine {

	public void beforeClass() {

	}

	@Test
	public void testGenerateSimple() {

		JPQLQueryEngine qe = new JPQLQueryEngine();
		QueryByFilter qbf = new QueryByFilter(Roma.class);
		qbf.addItem("test", QueryOperator.EQUALS, true);
		qbf.addItem("test2", QueryOperator.EQUALS, new Object());
		StringBuilder query = new StringBuilder();
		List<Object> params = new ArrayList<Object>();

		qe.buildQuery(qbf, query, params);

		Assert.assertEquals(replaceSpaces("select from org.romaframework.core.Roma A where A.test = ?1 and A.test2 = ?2"), replaceSpaces(query.toString()));
		Assert.assertEquals(params.size(), 2);
	}

	@Test
	public void testGenerateComplex() {

		JPQLQueryEngine qe = new JPQLQueryEngine();
		QueryByFilter qbf = new QueryByFilter(Roma.class);
		qbf.addItem("test", QueryOperator.EQUALS, true);
		qbf.addItem("test2", QueryOperator.EQUALS, new Object());
		qbf.addItem("test3", QueryOperator.LIKE, new Object());
		qbf.addItem("test3", QueryOperator.NOT_EQUALS, new Object());
		qbf.addItem("test3", QueryOperator.IN, new Object());
		StringBuilder query = new StringBuilder();
		List<Object> params = new ArrayList<Object>();
		qe.buildQuery(qbf, query, params);

		Assert.assertEquals(replaceSpaces("select from org.romaframework.core.Roma A where A.test = ?1 and A.test2 = ?2 and A.test3 LIKE ?3 and A.test3 <> ?4 and A.test3 IN ?5"),
				replaceSpaces(query.toString()));
		Assert.assertEquals(params.size(), 5);
	}

	@Test
	public void testGenerateFull() {

		JPQLQueryEngine qe = new JPQLQueryEngine();
		QueryByFilter qbf = new QueryByFilter(Roma.class);
		qbf.addItem("test", QueryOperator.EQUALS, true);
		qbf.addItem("test", QueryOperator.CONTAINS, new Object());
		qbf.addItem("test", QueryOperator.IN, new Object());
		qbf.addItem("test", QueryOperator.LIKE, new Object());
		qbf.addItem("test", QueryOperator.MAJOR, new Object());
		qbf.addItem("test", QueryOperator.MAJOR_EQUALS, new Object());
		qbf.addItem("test", QueryOperator.MINOR, new Object());
		qbf.addItem("test", QueryOperator.MINOR_EQUALS, new Object());
		qbf.addItem("test", QueryOperator.NOT_EQUALS, new Object());
		qbf.addItem("test", QueryOperator.NOT_IN, new Object());

		StringBuilder query = new StringBuilder();
		List<Object> params = new ArrayList<Object>();
		qe.buildQuery(qbf, query, params);

		Assert
				.assertEquals(
						replaceSpaces("select from org.romaframework.core.Roma A where A.test = ?1 and A.test IN ?2 and A.test IN ?3 and A.test LIKE ?4 and A.test > ?5 and A.test >= ?6 and A.test < ?7 and A.test <= ?8 and A.test <> ?9 and A.test NOT IN ?10"),
						replaceSpaces(query.toString()));
		Assert.assertEquals(params.size(), 10);
	}

	@Test
	public void testGenerateReverse() {

		JPQLQueryEngine qe = new JPQLQueryEngine();
		QueryByFilter qbf = new QueryByFilter(Roma.class);
		qbf.addItem("test", QueryOperator.EQUALS, true);
		QueryByFilter qbf1 = new QueryByFilter(Roma.class);
		qbf1.addItem("name", QueryOperator.EQUALS, true);
		qbf.addReverseItem(qbf1, "refer");
		qbf.addItem("name", QueryOperator.EQUALS, true);
		StringBuilder query = new StringBuilder();
		List<Object> params = new ArrayList<Object>();
		qe.buildQuery(qbf, query, params);

		Assert.assertEquals(replaceSpaces("select from org.romaframework.core.Roma A,org.romaframework.core.Roma B where A.test = ?1 and B.refer = A and B.name = ?2 and A.name = ?3"),
				replaceSpaces(query.toString()));
		Assert.assertEquals(params.size(), 3);
	}

	@Test
	public void testGenerateGroup() {

		JPQLQueryEngine qe = new JPQLQueryEngine();
		QueryByFilter qbf = new QueryByFilter(Roma.class);
		qbf.addItem("test", QueryOperator.EQUALS, true);
		QueryByFilterItemGroup group = qbf.addGroup(QueryByFilterItemGroup.PREDICATE_OR);
		group.addItem("test", QueryOperator.EQUALS, new Object());
		group.addItem("test1", QueryOperator.EQUALS, new Object());
		StringBuilder query = new StringBuilder();
		List<Object> params = new ArrayList<Object>();
		qe.buildQuery(qbf, query, params);

		Assert.assertEquals(replaceSpaces("select from org.romaframework.core.Roma A where A.test = ?1 and (A.test = ?2 or A.test1 = ?3)"),
				replaceSpaces(query.toString()));
		Assert.assertEquals(params.size(), 3);
	}

	
	@Test
	public void testGenerateProjections() {

		JPQLQueryEngine qe = new JPQLQueryEngine();
		QueryByFilter qbf = new QueryByFilter(Roma.class);
		qbf.addProjection("test");
		qbf.addProjection("test",ProjectionOperator.COUNT);
		qbf.addProjection("test",ProjectionOperator.MAX);
		qbf.addProjection("test",ProjectionOperator.MIN);
		qbf.addProjection("test",ProjectionOperator.AVG);
		qbf.addItem("test", QueryOperator.EQUALS, true);
		StringBuilder query = new StringBuilder();
		List<Object> params = new ArrayList<Object>();
		qe.buildQuery(qbf, query, params);

		Assert.assertEquals(replaceSpaces("select A.test,COUNT(A.test),MAX(A.test),MIN(A.test),AVG(A.test) from org.romaframework.core.Roma A where A.test = ?1 group by A.test"),
				replaceSpaces(query.toString()));
		Assert.assertEquals(params.size(), 1);
	}

	
	private String replaceSpaces(String toReplace) {
		int size;
		do {
			size = toReplace.length();
			toReplace = toReplace.replace("  ", " ");

		} while (size != toReplace.length());
		return toReplace;
	}
}
