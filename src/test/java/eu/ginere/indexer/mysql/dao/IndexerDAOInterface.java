package eu.ginere.indexer.mysql.dao;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.base.util.test.TestInterface;

public interface IndexerDAOInterface extends TestInterface{

	/**
	 * This search into the database the result for a token and a type.
	 * 
	 * @param token The token to search
	 * @param type The type of the token to serach May be null ?
	 * @param first The firs element of the result list to retreive
	 * @param number The max number of elements to retrive
	 * @return
	 * @throws DaoManagerException
	 */
	public List<IndexerResult> search(String token,String type,int firstElement,int number) throws DaoManagerException;

	public List<IndexerResult> searchTypeBis(Collection<String> sortedTokens,
			String type, int firstElement, int pageSize) throws DaoManagerException;

	/**
	 * Add a new indexer Element into the database
	 * @param indx
	 */
	public void insert(IndexerElement indexerElement)throws DaoManagerException;

	public void createTokenCountInner(String type)throws DaoManagerException;

	public void updateTokenCountInner(String type) throws DaoManagerException;

	
	/**
	 * Returns the last time an object has been modified or null if there is no object inserted
	 * 
	 * @param type
	 * @return the last date an object is inserted or null if no object has been inserted
	 * @throws DaoManagerException
	 */
	public Date getLastUpdate(String type) throws DaoManagerException;

}
