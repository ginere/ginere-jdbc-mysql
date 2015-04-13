package eu.ginere.jdbc.mysql.dao.cache;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.base.util.dao.cache.KeyCacheObject;
import eu.ginere.base.util.dao.cache.impl.AbstractKeyCacheManager;
import eu.ginere.base.util.dao.cache.impl.KeyCacheManagerWatcher;
import eu.ginere.jdbc.mysql.dao.KeyDAOInterface;

/**
 * @author ventura
 *
 * Clase abstracta para todos los manager que gestionan objetos basados en un ID.
 * Utiliza un dao Existente.
 */
public class AbstractSQLKeyCacheManager<T extends KeyCacheObject> extends AbstractKeyCacheManager<T>{

	private final KeyDAOInterface<T> dao;
	
	protected AbstractSQLKeyCacheManager(KeyDAOInterface<T> dao){
		this.dao=dao;
	}

	protected AbstractSQLKeyCacheManager(KeyDAOInterface<T> dao,KeyCacheManagerWatcher<T> watcher){
		super(watcher);
		this.dao=dao;
	}


	protected T getInner(String id) throws DaoManagerException{		
		return dao.get(id);
	}
	
	protected T getInner(String id,T defaultValue) throws DaoManagerException{		
		return dao.get(id,defaultValue);
	}
	/**
	 * @param id
	 * 
	 * @return This is called to remove object from backend
	 */
	protected void removeInner(String id) throws DaoManagerException{
		dao.delete(id);
	}

	/**
	 * Return true if the object exists
	 * @param id
	 * @return
	 */
	protected boolean existsInnner(String id)throws DaoManagerException{
		return dao.exists(id);
	}
	
	@Override
	public long getBackendElementNumber() throws DaoManagerException {
		return dao.count();
	}

	@Override
	protected String insertInner(String id, T obj) throws DaoManagerException {
		return dao.insert(id,obj);	
	}

	@Override
	protected String insertInner(T obj) throws DaoManagerException {
		return dao.insert(obj);	
	}

	@Override
	protected void updateInner(T obj) throws DaoManagerException {
		dao.update(obj);		
	}

	@Override
	protected void removeAllInner() throws DaoManagerException {
		dao.deleteAll();
	}	
}
