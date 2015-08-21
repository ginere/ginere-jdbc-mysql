////HEAD
//package eu.ginere.indexer.mysql.dao;
//
//import java.util.Collection;
//import java.util.HashSet;
//import java.util.List;
//
//import eu.ginere.base.util.dao.DaoManagerException;
//import eu.ginere.base.util.test.TestInterface;
//
//public interface TokenDAOInterface extends TestInterface{
//
//	/**
//	 * This search into the database the result for a token and a type.
//	 * 
//	 * @param token The token to search
//	 * @param type The type of the token to serach May be null ?
//	 * @param first The firs element of the result list to retreive
//	 * @param number The max number of elements to retrive
//	 * @return
//	 * @throws DaoManagerException
//	 */
//	public int getCount(String token,String type) throws DaoManagerException;
//
//
//	public int getCount(HashSet<String> tokensSet, String type) throws DaoManagerException;
//	
//	/**
//	 * This sort the tokens depending on the number of occurences
//	 *  
//	 * @param tokensSet
//	 * @param type
//	 * @return
//	 */
//	public Collection<String> sort(HashSet<String> tokensSet, String type) throws DaoManagerException;
//
//
//	public List<String> getAutoComplete(String token, String type,
//			int numberOfElements) throws DaoManagerException;
//
//
//	/**
//	 * Update the token count for each token
//	 * 
//	 * @throws DaoManagerException
//	 */
//	public void updateTokenCount(String type)throws DaoManagerException;
//
//
//}
///*
//=======
//package eu.ginere.indexer.dao;
//
//import java.util.Collection;
//import java.util.HashSet;
//import java.util.List;
//
//import eu.ginere.base.util.dao.DaoManagerException;
//import eu.ginere.base.util.test.TestInterface;
//
//public interface TokenDAOInterface extends TestInterface{
//
//	**
//	 * This search into the database the result for a token and a type.
//	 * 
//	 * @param token The token to search
//	 * @param type The type of the token to serach May be null ?
//	 * @param first The firs element of the result list to retreive
//	 * @param number The max number of elements to retrive
//	 * @return
//	 * @throws DaoManagerException
//	 *
//	public int getCount(String token,String type) throws DaoManagerException;
//
//
//	public int getCount(HashSet<String> tokensSet, String type) throws DaoManagerException;
//	
//	**
//	 * This sort the tokens depending on the number of occurences
//	 *  
//	 * @param tokensSet
//	 * @param type
//	 * @return
//	 *
//	public Collection<String> sort(HashSet<String> tokensSet, String type) throws DaoManagerException;
//
//
//	public List<String> getAutoComplete(String token, String type,
//			int numberOfElements) throws DaoManagerException;
//
//
//	**
//	 * Update the token count for each token
//	 * 
//	 * @throws DaoManagerException
//	 *
//	public void updateTokenCount(String type)throws DaoManagerException;
//
//
//}
//branch 'master' of https://ginere@github.com/ginere/ginere-indexer-core.git
//*/