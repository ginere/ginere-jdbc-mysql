package avem.jdbc.test;

import java.util.List;

import org.junit.Test;

import avem.common.util.dao.KeyDTO;
import avem.jdbc.dao.ParentQueryDAO;

public abstract class ParentQueryDAOTest<T extends KeyDTO> extends AbstractSQLDAOTest {

	protected ParentQueryDAOTest(ParentQueryDAO<T> DAO){
		super(DAO,false);
		
	}

	protected ParentQueryDAOTest(ParentQueryDAO<T> DAO,boolean removeBackEnd){
		super(DAO,removeBackEnd);
		
	}
	
	@Test
	public void testKeyBackEnd() throws Exception {		
		try {
			setDataSource();
			ParentQueryDAO<T> keyDAO=(ParentQueryDAO<T>)DAO;
				
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

//	@Test
//	public void testInsert() throws Exception {		
//		try {
//			setDataSource();
//			ParentQueryDAO<T> keyDAO=(ParentQueryDAO<T>)DAO;
//			
//			T obj=getTestObj();
//			String id=obj.getId();
//			
//			// First erase objects from old tests
//			if (id!=null && keyDAO.exists(id)){
//				keyDAO.delete(id);
//			}
//			
//			// Create a new Object
//			String ret=keyDAO.insert(obj);
//			
//			if (id==null){
//				id=ret;
//			}
//			
//			assertNotNull(id);
//			assertTrue(keyDAO.exists(id));
//
//			// get the object
//			T readed=keyDAO.get(id);
//
//			// update the object
//			keyDAO.update(readed);
//
//			// Get All
//			List <I>list=keyDAO.getAll();
//			assertTrue(list.size()>0);
//
//			List <String>listIds=keyDAO.getAllIds();
//			assertTrue(listIds.size()>0);
//
//			long count=keyDAO.count();
//
//			assertTrue(list.size()==count);
//			assertTrue(listIds.size()==count);
//
//
//			// conditions
//			List <I>listConditions=keyDAO.getByConditions(" AND "+keyDAO.getTableName()+'.'+keyDAO.getKeyColumnName()+"='"+id+"' ");
//			assertTrue(listConditions.size()==1);
//
//
//			// finaly delete old objects
//			keyDAO.delete(id);
//
//		} catch (Exception e) {
//			log.error("", e);
//			throw e;
//		}
//	}
//
//	protected abstract T getTestObj() throws DaoManagerException;
}
