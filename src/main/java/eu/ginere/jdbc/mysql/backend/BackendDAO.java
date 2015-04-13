package eu.ginere.jdbc.mysql.backend;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.jdbc.mysql.dao.AbstractKeyObjectSQLDAO;

public class BackendDAO extends AbstractKeyObjectSQLDAO<BackendInfo>{
	
	static final private String CREATE_QUERY_ARRAY[][]=new String[][]{
		{ //V1
			"CREATE TABLE COMMON_BACKEND ("
			+ "		ID varchar(255) NOT NULL,"
			+ "		VERSION int(8) NOT NULL,"
			+ "		LAST_CHANGE TIMESTAMP NOT NULL,"
			+ "		LAST_UPDATED bigint unsigned NOT NULL,"
			+ "		PRIMARY KEY (ID)"
//			+ "		UNIQUE KEY ID (ID),"
//			+ "		KEY ID_2 (ID)"
			+ ");",
		},		
	};

	static final private String TABLE_NAME= "COMMON_BACKEND";
	static final private String KEY_COLUMN= "ID";
	static final private String[] COLUMNS_MINUS_COLUMNS_NAME=
		new String[] {
			"VERSION",
			"LAST_CHANGE",
			"LAST_UPDATED",
	};


	public static final BackendDAO DAO=new BackendDAO();
	
	
	private BackendDAO() {
		super(TABLE_NAME,KEY_COLUMN,COLUMNS_MINUS_COLUMNS_NAME,CREATE_QUERY_ARRAY,null);
	}


	@Override
	public BackendInfo createFromResultSet(String id, ResultSet rset,String query)
			throws SQLException, DaoManagerException {
		return new BackendInfo(id,
							   getInt(rset,"VERSION",query),
//							   getDate(rset,"LAST_CHANGE",query)
							   getLong(rset,"LAST_UPDATED",query)
							   );
	}
	
	@Override
	protected int setInsertColumns(PreparedStatement pstm,
								   BackendInfo obj, 
								   int firstIndex,
								   String query)throws DaoManagerException {
		int i=firstIndex;
		
		setInt(pstm,i++, obj.getVersion(), query);
		setDate(pstm,i++, new Date(obj.lastUpdate()), query);
		setLong(pstm,i++, obj.lastUpdate(), query);
		return i;
	}
}
