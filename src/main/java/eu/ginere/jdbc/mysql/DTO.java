package eu.ginere.jdbc.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import eu.ginere.base.util.dao.DaoManagerException;


/**
 * @author ventura
 *
 * Clase madra para todos los objetos producidos por un DAO
 */
public abstract class DTO {
	
	protected String getString(ResultSet rset,String columnName,String tableName) throws DaoManagerException{
		try {
			return rset.getString(columnName);
		}catch(SQLException e){
			throw new DaoManagerException("columnName:"+columnName+" tableName:"+tableName,e);
		}
	}

	protected int getInt(ResultSet rset,String columnName,String tableName) throws DaoManagerException{
		try {
			return rset.getInt(columnName);
		}catch(SQLException e){
			throw new DaoManagerException("columnName:"+columnName+" tableName:"+tableName,e);
		}
	}
	
	protected boolean getBoolean(ResultSet rset,String columnName,String tableName) throws DaoManagerException{
		try {
			return rset.getBoolean(columnName);
		}catch(SQLException e){
			throw new DaoManagerException("columnName:"+columnName+" tableName:"+tableName,e);
		}
	}
	
	protected long getLong(ResultSet rset,String columnName,String tableName) throws DaoManagerException{
		try {
			return rset.getLong(columnName);
		}catch(SQLException e){
			throw new DaoManagerException("columnName:"+columnName+" tableName:"+tableName,e);
		}
	}
	

	protected double getDouble(ResultSet rset,String columnName,String tableName) throws DaoManagerException{
		try {
			return rset.getDouble(columnName);
		}catch(SQLException e){
			throw new DaoManagerException("columnName:"+columnName+" tableName:"+tableName,e);
		}
	}
	
	protected Date getDate(ResultSet rset,String columnName,String tableName) throws DaoManagerException{
		try {
			return rset.getTimestamp(columnName);
		}catch(SQLException e){
			throw new DaoManagerException("columnName:"+columnName+" tableName:"+tableName,e);
		}
	}
	
	public String toString() {
//		return ToStringBuilder.reflectionToString(this);
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
	}
}
