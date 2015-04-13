package avem.jdbc.backend;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.jdbc.mysql.backend.BackendDAO;
import eu.ginere.jdbc.mysql.backend.BackendInfo;
import eu.ginere.jdbc.mysql.dao.test.AbstractKeyObjectSQLDAOTest;

public class BackendDAOTest2 extends AbstractKeyObjectSQLDAOTest<BackendInfo>{
	
	public BackendDAOTest2(){
		super(BackendDAO.DAO,false);
	}

	@Override
	protected BackendInfo getTestObj() {
		return new BackendInfo("testId", 1);
	}
	
	protected void cleanTestBeforeStart() throws DaoManagerException{
		// do nothing
		
	}
}
