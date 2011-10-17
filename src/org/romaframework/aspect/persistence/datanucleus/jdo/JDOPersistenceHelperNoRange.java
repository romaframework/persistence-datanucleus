package org.romaframework.aspect.persistence.datanucleus.jdo;

import java.util.List;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.romaframework.aspect.persistence.PersistenceException;

public class JDOPersistenceHelperNoRange extends JDOPersistenceHelper{

	public List<?> prepareQueryInternal(PersistenceManager iManager, org.romaframework.aspect.persistence.Query iQuerySource,
			Query iQueryToExecute, Query iCountQuery, Map<String, Object> parValues) {

		try {
			// FIRST PAGE: GET RESULT NUMBER
			if (iQuerySource.getRangeFrom() == 0) {

				// BEFORE TO EXECUTE COUNT TOTAL ITEMS
				// iCountQuery.setUnique(true);
				// iCountQuery.setResult("count(this)");
				//
				// long totalItems = -1;
				//
				// if (parValues != null)
				// totalItems = (Long) iCountQuery.executeWithMap(parValues);
				// else
				// totalItems = (Long) iCountQuery.execute();

				// List<?> res = null;
				// if (parValues != null)
				// res = (List) iCountQuery.executeWithMap(parValues);
				// else
				// res = (List) iCountQuery.execute();
				//
				// iQuerySource.setTotalItems(res == null ? 0 : res.size());
				//
				// iCountQuery.closeAll();
			}

			List<?> partialResult;
			if (parValues != null)
				partialResult = (List<?>) iQueryToExecute.executeWithMap(parValues);
			else
				partialResult = (List<?>) iQueryToExecute.execute();

			iQuerySource.setTotalItems(partialResult.size());

//			if (iQuerySource.getRangeFrom() > -1 && iQuerySource.getRangeTo() > -1)
//				iQueryToExecute.setRange(iQuerySource.getRangeFrom(), iQuerySource.getRangeTo());

			// EXECUTE QUERY CONSIDERING PAGING
			List<?> result;
			if (parValues != null)
				result = (List<?>) iQueryToExecute.executeWithMap(parValues);
			else
				result = (List<?>) iQueryToExecute.execute();
			
			if (iQuerySource.getRangeFrom() > -1 && iQuerySource.getRangeTo() > -1){
				int min = Math.min(result.size(),  iQuerySource.getRangeFrom());
				int max = Math.min(result.size(),  iQuerySource.getRangeTo());
				result = result.subList(min, max);
			}

			return result;
		} catch (Throwable e) {
			log.error("[JDOPersistenceAspect.prepareQuery]", e);
			throw new PersistenceException("prepareQuery", e);
		}
	}
}
