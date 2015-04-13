package avem.jdbc.backend;

import avem.common.util.dao.DaoManagerException;
import avem.jdbc.test.AbstractKeyObjectSQLDAOTest;

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
