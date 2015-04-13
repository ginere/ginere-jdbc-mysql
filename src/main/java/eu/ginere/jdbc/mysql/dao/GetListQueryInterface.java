package eu.ginere.jdbc.mysql.dao;

import java.sql.PreparedStatement;


public interface GetListQueryInterface{
	public String getQuery();
	public void setAttributes(PreparedStatement pstm,String query);
}