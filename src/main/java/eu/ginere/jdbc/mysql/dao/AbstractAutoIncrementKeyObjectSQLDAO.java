package eu.ginere.jdbc.mysql.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.base.util.dao.KeyAutoincrementDTO;

/**
 * @author ventura
 *
 * DAO para ser usuado por objetos que gestionan su identificacion por una secuencia autoincrementado como Groups
 *
 * @param <I>
 * @param <T>
 */
public abstract class AbstractAutoIncrementKeyObjectSQLDAO<T extends KeyAutoincrementDTO> extends AbstractKeyObjectSQLDAO<T> implements KeyDAOInterface<T>{

	protected final String INSERT_QUERY_AUTO_INCREMENT;
	
	protected AbstractAutoIncrementKeyObjectSQLDAO(String tableName,
												   String keyColumnName,
												   String columnsArrayMinusKeyColumnName[],
												   String createQueryArray[][],
												   String deleteQueryArray[]){
		super(tableName,keyColumnName,columnsArrayMinusKeyColumnName,createQueryArray,deleteQueryArray);

		// Auton Incremente
		StringBuilder insertBuilder=new StringBuilder();
		insertBuilder.append("INSERT INTO ");
		insertBuilder.append(tableName);
		insertBuilder.append("(");
		insertBuilder.append(COLUMNS_MINUS_COLUMN_NAME);
		insertBuilder.append(") VALUES (");

		// then the rest of the column
		for (int i=0;i<columnsArrayMinusKeyColumnName.length;i++){
			if (i<columnsArrayMinusKeyColumnName.length-1){
				insertBuilder.append("?,");
			}else {
				insertBuilder.append("?");
			}
		}
		insertBuilder.append(")");
		
		this.INSERT_QUERY_AUTO_INCREMENT=insertBuilder.toString();
	}

	
	/**
	 * The Id of the object to insert may be null we assume that the id column is one auto_increment colum
	 * @param interf
	 * @return
	 * @throws DaoManagerException
	 */
	public String insertAutoIncrement(Connection connection,T interf) throws DaoManagerException {
		String query=INSERT_QUERY_AUTO_INCREMENT;
		
		try {				
			PreparedStatement pstmInsert = getPrepareStatement(connection,
															   query);
            try {
                setInsertFromAutoIncrementKeyColumnStatement(pstmInsert,interf,query);
			
                executeUpdate(pstmInsert, query);
                
                try {
                    String queryLast="SELECT LAST_INSERT_ID()";
                    PreparedStatement pstmLast = getPrepareStatement(connection,
                                                                     queryLast);
                    try {
                        ResultSet rset=executeQuery(pstmLast,queryLast);
                        try {
                            if(rset.next()){
                                String id=rset.getString(1);
                                interf.setId(id);
                                
                                return id;
                            }else{
                                throw new DaoManagerException("While insert object:"+interf+" LAST_INSERT_ID return no value!!!");
                            }
                        }finally{
                            close(rset);
                        }                        
                    }finally{
                        close(pstmLast);
                    }
                }catch (SQLException e) {
                    throw new DaoManagerException("While getting LAST_INSERT_ID from insert object:"+interf);
                }
            }finally{
                close(pstmInsert);
            }

		} catch (DaoManagerException e) {
			String error = "Insert object:'" + interf + 
				"' query:'" + query+
				"'";
			
			throw new DaoManagerException(error, e);
		}		
	}
	
	public String insertAutoIncrement(T interf) throws DaoManagerException {
		Connection connection = getConnection();
		
		try {				
			return insertAutoIncrement(connection,interf);
		} finally {
			closeConnection(connection);
		}		
	}

	public String insert(T interf) throws DaoManagerException {
		return insertAutoIncrement(interf);
	}
	
	public String insert(Connection connection,T interf) throws DaoManagerException {
		return insertAutoIncrement(connection,interf);
	}
	

	public String insert(String id,T interf) throws DaoManagerException {
		log.warn("Why this function is called? for id:"+id+" and object"+interf);
		return insertWithValidKey(interf);
	}

	protected void setInsertFromAutoIncrementKeyColumnStatement(PreparedStatement pstm,
																T obj,
																String query) throws DaoManagerException{
		setInsertColumns(pstm, obj, 1,query);
	}
}
