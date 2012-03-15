package org.romaframework.aspect.persistence.datanucleus.jdo;

import org.romaframework.aspect.persistence.QueryByFilter;
import org.romaframework.aspect.persistence.QueryByFilterProjection;

public class ReverseQueryByFilterProject extends QueryByFilterProjection {

	private QueryByFilterProjection	wrapped;
	private QueryByFilter						reverse;

	public ReverseQueryByFilterProject(QueryByFilter reverse, QueryByFilterProjection wrapped) {
		super(null, null);
		this.reverse = reverse;
		this.wrapped = wrapped;
	}

	public String getField() {
		return wrapped.getField();
	}

	public ProjectionOperator getOperator() {
		return wrapped.getOperator();
	}

	public QueryByFilter getReverse() {
		return reverse;
	}
}
