package eu.ginere.jdbc.mysql.dao.test;

import java.util.List;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.base.util.dao.jdbc.KeyDTO;
import eu.ginere.jdbc.mysql.dao.AbstractKeyDao;

public abstract class AbstractKeyDaoTest<T extends KeyDTO> extends AbstractSQLDAOTest {

	protected AbstractKeyDaoTest(AbstractKeyDao<T> DAO){
		super(DAO,false);
		
	}

	protected AbstractKeyDaoTest(AbstractKeyDao<T> DAO,boolean removeBackEnd){
		super(DAO,removeBackEnd);
	}

	
	public void innerTestKeyBackEnd() throws Exception {		
		try {
			setDataSource();
			AbstractKeyDao<T> keyDAO=(AbstractKeyDao<T>)DAO;
				
			List<T> listAll=keyDAO.getAll();
			List<String> listAllIds=keyDAO.getAllIds();
			long elementNumber=keyDAO.getBackendElementNumber();
			
			assertEquals(elementNumber, listAll.size());
			assertEquals(elementNumber, listAllIds.size());
			
			if (listAllIds.size() >0 ){
				String id=listAllIds.get(0);
				
				assertTrue(keyDAO.exists(id));
				
				Object obj=keyDAO.get(id);
				
				assertTrue(obj!=null);
			}
			
		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}

	public void innerTestInsert() throws Exception {		
		try {
			setDataSource();
			AbstractKeyDao<T> keyDAO=(AbstractKeyDao<T>)DAO;
			
			cleanTestBeforeStart();
			T obj=getTestObj();
			String id=obj.getKey();
			
			// First erase objects from old tests
			if (id!=null && keyDAO.exists(id)){
				keyDAO.delete(id);
			}
			
			// Create a new Object
			String ret=keyDAO.insert(obj);
			
			if (id==null){
				id=ret;
			}
			
			assertNotNull(id);
			assertTrue(keyDAO.exists(id));

			// get the object
			T readed=keyDAO.get(id);

			// update the object
			keyDAO.update(readed);

			// Get All
			List <T>list=keyDAO.getAll();
			assertTrue(list.size()>0);

			List <String>listIds=keyDAO.getAllIds();
			assertTrue(listIds.size()>0);

			long count=keyDAO.count();

			assertTrue(list.size()==count);
			assertTrue(listIds.size()==count);


			// conditions
			List <T>listConditions=keyDAO.getByConditions(" where "+keyDAO.getKeyColumnName()+"='"+id+"' ");
			assertTrue(listConditions.size()==1);


			// finaly delete old objects
			keyDAO.delete(id);

		} catch (Exception e) {
			log.error("", e);
			throw e;
		}
	}

	protected abstract T getTestObj() throws DaoManagerException;
	protected abstract void cleanTestBeforeStart() throws DaoManagerException;
	
	protected void executeText() throws Exception {		
		innerTestBackEnd();		
		innerTestKeyBackEnd();
		
		// Test the DAO dependencies
		DAO.test();
		
		innerTestInsert();
	}
}
