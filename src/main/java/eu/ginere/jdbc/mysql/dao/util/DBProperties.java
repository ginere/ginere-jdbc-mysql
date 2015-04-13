package eu.ginere.jdbc.mysql.dao.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import eu.ginere.base.util.dao.DaoManagerException;
import eu.ginere.jdbc.mysql.dao.AbstractSQLDAO;


public class DBProperties extends AbstractSQLDAO {
	static final Logger log = Logger.getLogger(DBProperties.class);

	private static final String TABLE_NAME = "COMMON_PROPERTIES";
	private static final String PROPERTY_NAME = "ID";
	private static final String PROPERTY_VALUE = "PROPERTY_VALUE";
	private static final String FECHA_INSERCION = "CREATED";

	static final private String CREATE_QUERY_ARRAY[][]=new String[][]{
		{ // V1
			"CREATE TABLE "+TABLE_NAME+" ("
			+ "		ID varchar(255) NOT NULL,"
			+ "		PROPERTY_VALUE varchar(4000) NOT NULL,"
			+ "		CREATED timestamp NOT NULL DEFAULT NOW(),"
			+ "		PRIMARY KEY (ID)"
//			+ "		UNIQUE KEY ID (ID),"
//			+ "		KEY ID_2 (ID)"
			+ ");",
		},
	};

	private static final String GET_PROPERTY = "select "
		+ PROPERTY_VALUE + " from " + TABLE_NAME
		+ " where "+PROPERTY_NAME+"=? limit 1";

	public static final DBProperties DAO=new DBProperties();
	
	private DBProperties(){
		super(TABLE_NAME,CREATE_QUERY_ARRAY);
	}
	
	public int getIntValue(Class c, String propertyName, int defaultValue) {
		try {
			String ret = getStringValue(c, propertyName,null);

			if (ret == null) {
				return defaultValue;
			} else {
				try {
					return Integer.parseInt(ret);
				} catch (NumberFormatException e) {
					return defaultValue;
				}
			}
		} catch (Exception e) {
			log.warn("getIntValue c:" + c + 
					 "' propertyName:'" + propertyName +
					 "' defaultValue:'" + defaultValue + 
					 "'", e);
			return defaultValue;
		}
	}

	public void setIntValue(Class c, String propertyName, int value) throws DaoManagerException {
		setStringValue(c,propertyName,Integer.toString(value));
	}

	public boolean getBooleanValue(Class section, 
										  String propertyName,
										  boolean defaultValue) {
		try {
			String ret = getStringValue(section, propertyName,null);
			return toBoolean(ret, defaultValue);
		} catch (Exception e) {
			log.warn("getIntValue c:" + section + 
					 "' propertyName:'" + propertyName + 
					 "' defaultValue:'" + defaultValue
					 + "'", e);
			return defaultValue;
		}
	}

	public void setBooleanValue(Class c, String propertyName, boolean value) throws DaoManagerException {
		setStringValue(c,propertyName,((value)?"true":"false"));
	}

	/**
	 * true: "true".</br> false: "false".</br>
	 */
	public static boolean toBoolean(String str, 
									boolean defaultValue) {
		if (StringUtils.equalsIgnoreCase(null, str)) {
			return defaultValue;
		} else if (StringUtils.equalsIgnoreCase("true", str)) {
			return true;
		} else if (StringUtils.equalsIgnoreCase("false", str)) {
			return false;
		} else if (StringUtils.equalsIgnoreCase("1", str)) {
			return true;
		} else if (StringUtils.equalsIgnoreCase("0", str)) {
			return false;
		} else {
			return defaultValue;
		}
	}

	public String getStringValue(Class c, String propertyName,String defaultValue) {
		try {
			return get(getPropertyName(c,propertyName), defaultValue);
		} catch (DaoManagerException e) {
			log.warn(" c:" + c + 
					 "' propertyName:'" + propertyName +
					 "' defaultValue:'" + defaultValue + 
					 "'", e);
			return defaultValue;
		}
	}
		
	public void setStringValue(Class c, String propertyName,String value) throws DaoManagerException {
		String name=getPropertyName(c,propertyName);

		if (!exists(name)){
			insert(name,value);
		} else {
			update(name,value);
		}
	}

	private static String getPropertyName(Class c, String propertyName) {
		return c.getName()+propertyName;
	}

	public String get(Class c,String propertyName,String defaultvalue) throws DaoManagerException {		
		return get(getPropertyName(c,propertyName),defaultvalue);
	}

	private String get(String propertyName,String defaultvalue) throws DaoManagerException {		
		Connection connection = getConnection();
		String query=GET_PROPERTY;

		try {
			PreparedStatement pstm = getPrepareStatement(connection,query);
            try {
                setString(pstm, 1, propertyName, query);
                
                return getStringFromQuery(pstm, query, defaultvalue);
                
//                ResultSet rset = executeQuery(pstm, query);
//                try {
//                    if (rset.next()){
//                        return rset.getString(PROPERTY_VALUE);
//                    } else {
//                        return defaultvalue;
//                    }
//                }finally{
//                    close(rset);
//                }
            }finally{
                close(pstm);
            }
//		} catch (SQLException e) {
//			String error = "propertyName:'"+propertyName+"'.";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
		} catch (DaoManagerException e) {
			String error = "propertyName:'"+propertyName+"'.";
			
			log.error(error, e);
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}

	public boolean exists(Class c,String propertyName) throws DaoManagerException {		
		String name=getPropertyName(c,propertyName);

		return exists(name);
	}

	private boolean exists(String propertyName) throws DaoManagerException {		
		Connection connection = getConnection();
		String query=GET_PROPERTY;

		try {
			PreparedStatement pstm = getPrepareStatement(connection,query);
			try {
				setString(pstm, 1, propertyName, query);
	
				return hasNext(pstm, query);
			}finally{
				close(pstm);
			}
//			ResultSet rset = executeQuery(pstm, query);
//			
//			if (rset.next()){
//				return true;
//			} else {
//				return false;
//			}
//		} catch (SQLException e) {
//			String error = "propertyName:'"+propertyName+"'.";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
		} catch (DaoManagerException e) {
			String error = "propertyName:'"+propertyName+"'.";
			
			log.error(error, e);
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}

	public void insert(Class c,String propertyName,String value) throws DaoManagerException {		
		insert(getPropertyName(c,propertyName),value);
	}


	private static final String INSERT_PROPERTY = "insert into "+TABLE_NAME+
	 "("+PROPERTY_NAME+","+PROPERTY_VALUE + 
		") VALUES (?,? )";

	private void insert(String propertyName,String value) throws DaoManagerException {		
		Connection connection = getConnection();
		String query=INSERT_PROPERTY;

		try {
			PreparedStatement pstm = getPrepareStatement(connection,query);
            try {
                setString(pstm, 1, propertyName, query);
                setString(pstm, 2, value, query);
                
                long number=executeUpdate(pstm, query);
                
                if (log.isDebugEnabled()){
                    log.debug("Rows modified:"+number);
                }
            }finally{
                close(pstm);
            }			
		} catch (DaoManagerException e) {
			String error = "propertyName:'"+propertyName+"' value:'"+value+"'.";
			
			log.error(error, e);
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}

	public void update(Class c,String propertyName,String value) throws DaoManagerException {		
		update(getPropertyName(c,propertyName),value);
	}

	private static final String UPDATE_PROPERTY = "update "+TABLE_NAME+
		" set "+PROPERTY_VALUE+"=?,"+FECHA_INSERCION + "=SYSDATE() where "+PROPERTY_NAME+
		"=?";

	private void update(String propertyName,String value) throws DaoManagerException {		
		Connection connection = getConnection();
		String query=UPDATE_PROPERTY;

		try {
			PreparedStatement pstm = getPrepareStatement(connection,query);
            try {
                setString(pstm, 1, value, query);
                setString(pstm, 2, propertyName, query);
                
                long number=executeUpdate(pstm, query);
                
                if (log.isDebugEnabled()){
                    log.debug("Rows modified:"+number);
                }
            }finally{
                close(pstm);
            }			
		} catch (DaoManagerException e) {
			String error = "propertyName:'"+propertyName+"' value:'"+value+"'.";
			
			log.error(error, e);
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}

	public void delete(Class c,String propertyName) throws DaoManagerException {		
		delete(getPropertyName(c,propertyName));
	}

	
	private static final String DELETE_PROPERTY = "delete from "+TABLE_NAME+
		" where "+PROPERTY_NAME+"=?";

	private void delete(String propertyName) throws DaoManagerException {		
		Connection connection = getConnection();
		String query=DELETE_PROPERTY;

		try {
			PreparedStatement pstm = getPrepareStatement(connection,query);
            try {
                setString(pstm, 1, propertyName, query);
                
                long number=executeUpdate(pstm, query);
                
                if (log.isDebugEnabled()){
                    log.debug("Rows modified:"+number);
                }
            }finally{
                close(pstm);
            }
		} catch (DaoManagerException e) {
			String error = "propertyName:'"+propertyName+"' .";
			
			log.error(error, e);
			throw new DaoManagerException(error, e);
		} finally {
			closeConnection(connection);
		}
	}
}

