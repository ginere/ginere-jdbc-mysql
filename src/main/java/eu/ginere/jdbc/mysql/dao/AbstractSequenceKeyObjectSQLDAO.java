package eu.ginere.jdbc.mysql.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.jdbc.mysql.KeyDTO;

/**
 * @author ventura
 *
 * DAO para ser usuados por objetos que utilizan una secuencia para generar su id como ficheros
 *
 * @param <I>
 * @param <T>
 */
public abstract class AbstractSequenceKeyObjectSQLDAO<T extends KeyDTO> extends AbstractKeyDao<T> implements KeyDAOInterface<T>{

	protected final String SEQUENCE_NAME;
	protected final String SEQUENCE_QUERY;
	
	protected AbstractSequenceKeyObjectSQLDAO(String tableName,
											  String keyColumnName,
											  String columnsArrayMinusKeyColumnName[],
											  String createQueryArray[][],
											  String deleteQueryArray[],
											  String SEQUENCE_NAME){
		super(tableName,keyColumnName,columnsArrayMinusKeyColumnName,createQueryArray,deleteQueryArray);

		this.SEQUENCE_NAME=SEQUENCE_NAME;

		StringBuilder buffer=new StringBuilder();
		buffer.append("UPDATE ");
		buffer.append(SEQUENCE_NAME);
		buffer.append(" SET id=LAST_INSERT_ID(id+1)");

		this.SEQUENCE_QUERY=buffer.toString();
	}

	public String insert(String id,T interf) throws DaoManagerException {
		Connection connection = getConnection();
		
		try {				
			return insert(connection,id,interf);
		} catch (DaoManagerException e) {
			String error = "Insert object:'" + interf + 
				"' id:'" + id+
				"'";
			
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}		
	}

	public String insert(T interf) throws DaoManagerException {
		Connection connection = getConnection();
		
		try {				
			return insert(connection,interf);
		} finally {
			closeConnection(connection);
		}		
	}

	public String insert(Connection connection,T interf) throws DaoManagerException {
		try {				
			long id=getNextValueFromSecuence(connection);
			return insert(connection,Long.toString(id),interf);
		} catch (DaoManagerException e) {
			String error = "Insert object:'" + interf + 
				"'";
			
			throw new DaoManagerException(error, e);
		}
	}
	
	protected String insert(Connection connection,String id,T interf) throws DaoManagerException {
		String query=INSERT_QUERY_VALID_KEY;
		
		try {				
			PreparedStatement pstmInsert = getPrepareStatement(connection,
															   query);
            try {
                setInsertStatementFromSequence(pstmInsert,id,interf,query);
                
                executeUpdate(pstmInsert, query);
                
                return id;
            }finally{
                close(pstmInsert);
            }
		} catch (DaoManagerException e) {
			String error = "Insert object:'" + interf + 
				"' id:'" + id+
				"' query:'" + query+
				"'";
			
			throw new DaoManagerException(error, e);
		}		
	}

	public long getNextValueFromSecuence()throws DaoManagerException {
		Connection connection=getConnection();
			
		try {
			return getNextValueFromSecuence(connection);
		} finally {
			closeConnection(connection);
		}
	}

	public long getNextValueFromSecuence(Connection connection) throws DaoManagerException {

		String query=SEQUENCE_QUERY;
		try {
			PreparedStatement pstm= connection.prepareStatement(query);
			try {
				executeUpdate(pstm,query);
			} finally {
				close(pstm);
			}
			
			pstm= connection.prepareStatement("SELECT LAST_INSERT_ID()");
			try {
				ResultSet rset=pstm.executeQuery("SELECT LAST_INSERT_ID()");
                try {
                    if(rset.next()){
                        return rset.getLong(1);
                    }else{
                        throw new DaoManagerException("While executeInsertQuery LAST_INSERT_ID return no value!!!");
                    }
                }finally{
                    close(rset);
                }
            } finally {
				close(pstm);
			}


		} catch (SQLException e) {
			throw new DaoManagerException("While getting next vel for sequence:'"+this.SEQUENCE_NAME+ "'",e);
		} 
	}


	/**
	 * The key Column Name
	 * @return
	 */
	public String getSequenceName(){
		return SEQUENCE_NAME;
	}


	@Override
	protected void setInsertStatement(PreparedStatement pstm, 
                                      T obj,
                                      String query) throws DaoManagerException {
		throw new IllegalAccessError("Why this function is called?");	
	}
	
	protected void setInsertStatementFromSequence(PreparedStatement pstm,
                                                  String id,
                                                  T obj,
                                                  String query) throws DaoManagerException{
		set(pstm,1, id, query);
		fillUpdateStatement(pstm, obj, 2,query);
	}
	
	@Override
	public void deleteBackEnd() throws DaoManagerException{
		super.deleteBackEnd();
		dropTable(SEQUENCE_NAME);
	}


}
