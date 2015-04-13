package eu.ginere.jdbc.mysql.dao;

import java.sql.ResultSet;
import java.sql.SQLException;

import eu.ginere.base.util.dao.DaoManagerException;


/**
 * @author ventura
 * DAOS that can identify object by a key column
 *
 */
public interface KeyDAOInterface<T> extends DAOInterface {

	/**
	 * The key Column Name
	 * @return
	 */
	public String getKeyColumnName();

	/**
	 * all the columns minus the key column
	 * @return
	 */
	public String[] getColumnsMinusKeyColumnName();
	
	/**
	 * Tiene que leer todas las columnas menos la columna de la primary key
	 */
	public T createFromResultSet(String id,ResultSet rset,String query) throws SQLException, DaoManagerException;
	

	public T get(String id) throws DaoManagerException;
	public T get(String id,T defaultValue) throws DaoManagerException;
	
	public void delete(String id)throws DaoManagerException;
	public void deleteAll()throws DaoManagerException;
	
	public boolean exists(String id) throws DaoManagerException;
	public long count() throws DaoManagerException;
	
	public String insert(String id,T object) throws DaoManagerException;
	
	public String insert(T object) throws DaoManagerException;
	
	public void update(T object) throws DaoManagerException;

}
