//package eu.ginere.indexer.mysql.dao;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Date;
//import java.util.List;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.log4j.Logger;
//
//import eu.ginere.base.util.dao.DaoManagerException;
//import eu.ginere.base.util.test.TestResult;
//import eu.ginere.jdbc.mysql.MySQLDataBase;
//import eu.ginere.jdbc.mysql.dao.AbstractDAO;
//
//public class IndexerDAO extends AbstractDAO implements IndexerDAOInterface{
//
//	static final Logger log = Logger.getLogger(IndexerDAO.class);
//	
//	static final public String TABLE_NAME= "INDEXER";
//	static final private String[] COLUMNS=new String[] {
//		"TOKEN", 
//		"TYPE",
//		"ID",
//		"LAST_UPDATE"
//	};
//
//	static final private String CREATE_QUERY_ARRAY[][]=new String[][]{
//		{ // V1
//			"CREATE TABLE "+TABLE_NAME+" ("
//			+ "		TOKEN varchar(128) NOT NULL,"
//			+ "		TYPE varchar(128) NOT NULL,"
//			+ "		ID varchar(1024) NOT NULL,"
//			+ "		LAST_UPDATE TIMESTAMP default SYSDATE NOT NULL"
//			+ ");",
//			"create index TYPE_TOKEN_INDEX ON INDEXER (TYPE,TOKEN);",
//		},
//	};
//	
//	static final private String CREATE_INDEX_ARRAY[][]=new String[][]{
//        //		SEARCH_ONE_START_TYPE
//		{ "TYPE_TOKEN_INDEX","ALTER TABLE "+TABLE_NAME+" ADD INDEX `TYPE_TOKEN_INDEX_ID` (TYPE, TOKEN, ID) ;"},
//		
//		};
//	
//	public static final IndexerDAO DAO=new IndexerDAO();
//
//	private IndexerDAO(){
//		super(TABLE_NAME,CREATE_QUERY_ARRAY);
//	}
//
//	
//	public static final String SEARCH_ONE="select ID from (SELECT ID,ROWNUM as NUMBER FROM "+TABLE_NAME+" where TYPE=? AND TOKEN=? order by ID ) WHERE NUMBER BETWEEN ? AND ?";
//    
//	@Override
//	public List<IndexerResult> search(String token, 
//                                      String type,
//                                      int firstElement, 
//                                      int number) throws DaoManagerException {
//		if (type == null || "".equals(type)){
//			return search(token, 
//						  firstElement, 
//						  number);
//		} else {
//
//			try {
//				Connection connection = getConnection();
//				String query=SEARCH_ONE;
//			
//				try {
//					PreparedStatement pstm = getPrepareStatement(connection,query);
//                
//					try {
//						int i=1;
//
//						setString(pstm, i++, type, query);
//						setString(pstm, i++, token, query);
//						setInt(pstm, i++, firstElement, query);
//						setInt(pstm, i++, firstElement+number, query);
//                    
//						ResultSet rset=executeQuery(pstm, query);
//						try {
//							List<IndexerResult> list= new ArrayList<IndexerResult>(rset.getFetchSize());
//                        
//							while (rset.next()){
//								String id=rset.getString(1);
//								IndexerResult result=new IndexerResult(type,id);
//								list.add(result);
//							}
//							return list;
//						}catch (SQLException e) {
//							throw new DaoManagerException("Query:'"+query+"'");
//						} finally {
//							close(rset);
//						}
//                    
//					} finally {
//						close(pstm);
//					}
//                    
//				} finally {
//					closeConnection(connection);
//				}
//			}catch (DaoManagerException e) {
//				throw new DaoManagerException("token:"+token+
//											  "' type:"+type+
//											  "' firstElement:"+firstElement+
//											  " number:"+number,e);
//			}
//		}
//	}
//
//	///>
//	public static final String SEARCH_ONE_NO_TYPE="select ID,TYPE from (SELECT ID,TYPE,ROWNUM as NUMBER FROM "+TABLE_NAME+" where TOKEN=? order by ID ) WHERE NUMBER BETWEEN ? AND ?";
//    
//	public List<IndexerResult> search(String token, 
//                                      int firstElement, 
//                                      int number) throws DaoManagerException {
//		try {
//			Connection connection = getConnection();
//			String query=SEARCH_ONE_NO_TYPE;
//			
//			try {
//				PreparedStatement pstm = getPrepareStatement(connection,query);
//                
//				try {
//					int i=1;
//					
//					setString(pstm, i++, token, query);
//					setInt(pstm, i++, firstElement, query);
//					setInt(pstm, i++, firstElement+number, query);
//                    
//					ResultSet rset=executeQuery(pstm, query);
//					try {
//						List<IndexerResult> list= new ArrayList<IndexerResult>(rset.getFetchSize());
//                        
//						while (rset.next()){
//							String id=rset.getString(1);
//							String type=rset.getString(2);
//							IndexerResult result=new IndexerResult(type,id);
//							list.add(result);
//						}
//						return list;
//					}catch (SQLException e) {
//						throw new DaoManagerException("Query:'"+query+"'");
//					} finally {
//						close(rset);
//					}
//                    
//				} finally {
//					close(pstm);
//				}
//                
//			} finally {
//				closeConnection(connection);
//			}
//		}catch (DaoManagerException e) {
//			throw new DaoManagerException("token:"+token+
//										  "' firstElement:"+firstElement+
//										  " number:"+number,e);
//		}
//	}
//
//	///<
//	
//	private static final String SELECT_LAST_DATE = "select max(LAST_UPDATE) from "+TABLE_NAME+" where type=?";
//	@Override
//	public Date getLastUpdate(String type) throws DaoManagerException {
//		Connection connection = getConnection();
//		String query=SELECT_LAST_DATE;
//	
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			try {
//				setString(pstm, 1, type, query);
//				
//				return getDate(pstm, query,null);	
//			} finally {
//				close(pstm);
//			}
//		} catch (DaoManagerException e) {
//			String error = "type:'"+type
//				+"'";
//			
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}		
//	}
//
//
//
//
//
//
//
//
//
//	private static final String INSERT = "insert into "+TABLE_NAME+
//		" (TOKEN,TYPE,ID) values (?,?,?)";
//
//	public void insert(IndexerElement indx) throws DaoManagerException{
//		insert(indx.getToken(),
//			   indx.getType(),
////			   indx.getField(),
//			   indx.getId());
//	}
//
//	public void insert(String token,String type,String id) throws DaoManagerException{
//		Connection connection = getConnection();
//		String query=INSERT;
//	
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			try {
//				setString(pstm, 1, token, query);
//				setString(pstm, 2, type, query);
//				//			setString(pstm, 3, field, query);
//				setString(pstm, 3, id, query);
//				
//				long number=executeUpdate(pstm, query);
//				
//				//			if (log.isDebugEnabled()){
//				//				log.debug("Rows modified:"+number);
//				//			}
//			} finally {
//				close(pstm);
//			}
//		} catch (DaoManagerException e) {
//			String error = "token:'"+token
//				+ "' type:'"+type
////				+ "' field:'"+field
//				+ "' id:'"+id+"'";
//			
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}		
//	}
////
////	private static final String DELETE = "delete from "+TABLE_NAME+
////	" where TYPE=? and ID=?";
////
////	public void delete(String type, String id) throws DaoManagerException{
////		Connection connection = getConnection();
////		String query=DELETE;
////		
////		try {
////			PreparedStatement pstm = getPrepareStatement(connection,query);
////			try {
////				setString(pstm, 1, type, query);
////				setString(pstm, 2, id, query);
////				// TODO
////				long number=executeUpdate(pstm, query);
////				
////				if (log.isDebugEnabled()){
////					log.debug("Rows modified:"+number);
////				}
////			} finally {
////				close(pstm);
////			}
////		} catch (DaoManagerException e) {
////			String error = " type:'"+type
////				+ "' id:'"+id+"'";
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);
////		} finally {
////			closeConnection(connection);
////		}		
////	}
//
//
//	private static final String SEARCH = "select type,id from "+TABLE_NAME+
//	" where token like ? ";
//	private static final String COUNT = "select count(*) from "+TABLE_NAME+
//	" where token like ?  ";
//
//	public List<IndexerResult> search(String searchString) throws DaoManagerException{
//		Connection connection = getConnection();
//		String query=SEARCH;
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			try {
//				setString(pstm, 1, searchString, query);
//				
//				ResultSet rset=executeQuery(pstm, query);
//				
//				List<IndexerResult> list= new ArrayList<IndexerResult>(rset.getFetchSize());
//				
//				while (rset.next()){
//					String token=rset.getString(1);
//					String type=rset.getString(2);
//					//				String field=rset.getString(3);
//					String id=rset.getString(3);
//					
//					IndexerResult result=new IndexerResult(type,id);
//					list.add(result);
//				}
//				return list;
//			} finally {
//				close(pstm);
//			}
//		} catch (SQLException e) {
//			String error = " searchString:'"+searchString+"'";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);			
//		} catch (DaoManagerException e) {
//			String error = " searchString:'"+searchString+"'";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}		
//	}
//	
////	public int count(String searchString) throws DaoManagerException{
////		Connection connection = getConnection();
////		String query=COUNT;
////		
////		try {
////			PreparedStatement pstm = getPrepareStatement(connection,query);
////			try {
////				setString(pstm, 1, searchString, query);
////				
////				return getIntFromQuery(pstm, query, 0);
////			} finally {
////				close(pstm);
////			}			
////		} catch (DaoManagerException e) {
////			String error = " searchString:'"+searchString+"'";
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);
////		} finally {
////			closeConnection(connection);
////		}		
////	}
//
//	
////	private static final String AUTO_COMPLETE = "SELECT token,count(*) as number FROM "+TABLE_NAME+" where token like ? group by token order by number desc LIMIT ?";
////
////	public List<String> getAutoComplete(String lastToken, int numberAutoComplete) throws DaoManagerException{
////		Connection connection = getConnection();
////		String query=AUTO_COMPLETE;
////		
////		try {
////			PreparedStatement pstm = getPrepareStatement(connection,query);
////			try {
////				//			String value=lastToken.replace(""%", '');
////				
////				setString(pstm, 1, lastToken+'%', query);
////				setInt(pstm, 2, numberAutoComplete, query);
////				
////				return getStringList(pstm, query);
////			} finally {
////				close(pstm);
////			}
////		} catch (DaoManagerException e) {
////			String error = " lastToken:'"+lastToken+"'";
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);
////		} finally {
////			closeConnection(connection);
////		}
////	}
//	
////	private static final String GET_TOKEN_LIST = "SELECT distinct(token) from  "+TABLE_NAME+" where type=?";
////
////	public List<String> getTokenList(String type) throws DaoManagerException{
////		Connection connection = getConnection();
////		String query=GET_TOKEN_LIST;
////		
////		try {
////			PreparedStatement pstm = getPrepareStatement(connection,query);
////			
////			setString(pstm, 1, type, query);
////			
////			return getStringList(pstm, query);			
////		} catch (DaoManagerException e) {
////			String error = " type:'"+type+"'";
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);
////		} finally {
////			closeConnection(connection);
////		}
////	}
//
////	private static final String GET_TOKEN_COUNT = "SELECT count(TOKEN) from  "+TABLE_NAME+" where token=? AND type=?";
////	public int getTokenCount(String token, String type) throws DaoManagerException{
////		Connection connection = getConnection();
////		String query=GET_TOKEN_COUNT;
////		
////		try {
////			PreparedStatement pstm = getPrepareStatement(connection,query);
////			try {
////				setString(pstm, 1, token, query);
////				setString(pstm, 2, type, query);
////				
////				return getIntFromQuery(pstm, query, 0);		
////			} finally {
////				close(pstm);
////			}
////		} catch (DaoManagerException e) {
////			String error = "token:'"+token+"' type:'"+type+"'";
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);
////		} finally {
////			closeConnection(connection);
////		}
////	}
//
////	private static final String SEARCH_VAR_START = "SELECT ID,TYPE,count(*) as number FROM "+TABLE_NAME+" where ";
////	// token='ana' or token='caballero'
////	private static final String SEARCH_VAR_END = " group by ID,TYPE order by number desc LIMIT ?,?";
////
////	public List<IndexerResult> search(HashSet <String>tokens,int first,int number) throws DaoManagerException{
////		Connection connection = getConnection();
////		StringBuilder buffer=new StringBuilder();
////
////		buffer.append(SEARCH_VAR_START);
////		for (int i=0;i<tokens.size();i++){
////			if (i != 0) {
////				buffer.append(" or ");			
////			}
////			buffer.append(" token=? ");
////		}
////		buffer.append(SEARCH_VAR_END);
////		String query=buffer.toString();
////		
////		
////		try {
////			PreparedStatement pstm = getPrepareStatement(connection,query);
////			try {
////				int i=1;
////				for (String token:tokens){
////					setString(pstm, i++, token, query);
////				}
////				setInt(pstm, i++, first, query);
////				setInt(pstm, i++, number, query);
////				
////				ResultSet rset=executeQuery(pstm, query);
////				
////				List<IndexerResult> list= new ArrayList<IndexerResult>(rset.getFetchSize());
////				
////				while (rset.next()){
////					//String token=rset.getString(1);
////					//				String field=rset.getString(3);
////					String id=rset.getString(1);
////					String type=rset.getString(2);
////					
////					IndexerResult result=new IndexerResult(type,id);
////					list.add(result);
////				}
////				return list;
////			} finally {
////				close(pstm);
////			}
////		} catch (SQLException e) {
////			String error = " searchString:'"+StringUtils.join(tokens, ",")+"' first:"+first+" number:"+number;
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);			
////		} catch (DaoManagerException e) {
////			String error = " searchString:'"+StringUtils.join(tokens, ",")+"' first:"+first+" number:"+number;
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);
////		} finally {
////			closeConnection(connection);
////		}		
////	}
//
//
//
//
////	private static final String SEARCH_VAR_START_TYPE = "SELECT ID,count(*) as number FROM "+TABLE_NAME+" where TYPE=? AND (";
////	// token='ana' or token='caballero'
////	private static final String SEARCH_VAR_END_TYPE = " ) group by ID order by number desc,ID asc LIMIT ?,?";
////
////	public List<IndexerResult> searchType(HashSet <String>tokens,String type,int first,int number) throws DaoManagerException{
////		Connection connection = getConnection();
////		StringBuilder buffer=new StringBuilder();
////
////		buffer.append(SEARCH_VAR_START_TYPE);
////		for (int i=0;i<tokens.size();i++){
////			if (i != 0) {
////				buffer.append(" or ");			
////			}
////			buffer.append(" token=? ");
////		}
////		buffer.append(SEARCH_VAR_END_TYPE);
////		String query=buffer.toString();
////		
////		
////		try {
////			PreparedStatement pstm = getPrepareStatement(connection,query);
////			try {
////				int i=1;
////				setString(pstm, i++, type, query);
////				for (String token:tokens){
////					setString(pstm, i++, token, query);
////				}
////				setInt(pstm, i++, first, query);
////				setInt(pstm, i++, number, query);
////				
////				ResultSet rset=executeQuery(pstm, query);
////				
////				List<IndexerResult> list= new ArrayList<IndexerResult>(rset.getFetchSize());
////				
////				while (rset.next()){
////					//String token=rset.getString(1);
////					//				String field=rset.getString(3);
////					String id=rset.getString(1);
////					//				String type=rset.getString(2);
////					
////					IndexerResult result=new IndexerResult(type,id);
////					list.add(result);
////				}
////				return list;
////			} finally {
////				close(pstm);
////			}
////		} catch (SQLException e) {
////			String error = " searchString:'"+StringUtils.join(tokens, ",")+"' first:"+first+" number:"+number;
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);			
////		} catch (DaoManagerException e) {
////			String error = " searchString:'"+StringUtils.join(tokens, ",")+"' first:"+first+" number:"+number;
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);
////		} finally {
////			closeConnection(connection);
////		}		
////	}
//
//
//	public List<IndexerResult> searchTypeBis(Collection<String>tokens,
//                                             String type,
//                                             int first,
//                                             int number) throws DaoManagerException{
//		if (type == null || "".equals(type)){
//			return searchTypeBis(tokens,
//								 first,
//								 number);
//		} else {
//			Connection connection = getConnection();
//			StringBuilder buffer=new StringBuilder();
//
//			for (int i=0;i<tokens.size();i++){
//				if (i == 0) {
//					buffer.append("SELECT ID FROM INDEXER where TOKEN=? AND TYPE=?");			
//				} else {
//					buffer=buffer.insert(0,"SELECT ID FROM INDEXER WHERE ID in (");
//				
//					buffer.append(") and TOKEN=? AND TYPE=?");
//				}
//			}
//			buffer.append(" order by ID desc LIMIT ?,? ");
//			String query=buffer.toString();
//		
//		
//			try {
//				PreparedStatement pstm = getPrepareStatement(connection,query);
//						
//				int i=1;
//				for (String token:tokens){
//					setString(pstm, i++, token, query);
//					setString(pstm, i++, type, query);
//				}
//				setInt(pstm, i++, first, query);
//				setInt(pstm, i++, number, query);
//	
//				ResultSet rset=executeQuery(pstm, query);
//	
//				List<IndexerResult> list= new ArrayList<IndexerResult>(rset.getFetchSize());
//			
//				while (rset.next()){
//					String id=rset.getString(1);
//
//					IndexerResult result=new IndexerResult(type,id);
//					list.add(result);
//				}
//				return list;
//			} catch (SQLException e) {
//				String error = " searchString:'"+StringUtils.join(tokens, ",")+"' first:"+first+" number:"+number;
//			
//				throw new DaoManagerException(error, e);			
//			} catch (DaoManagerException e) {
//				String error = " searchString:'"+StringUtils.join(tokens, ",")+"' first:"+first+" number:"+number;
//			
//				throw new DaoManagerException(error, e);
//			} finally {
//				closeConnection(connection);
//			}		
//		}
//	}
//	
//	
//	///>
//	
//
//	public List<IndexerResult> searchTypeBis(Collection<String>tokens,
//                                             int first,
//                                             int number) throws DaoManagerException{
//		Connection connection = getConnection();
//		StringBuilder buffer=new StringBuilder();
//		
//		for (int i=0;i<tokens.size();i++){
//			if (i == 0) {
//				buffer.append("SELECT ID,TYPE FROM INDEXER where TOKEN=? ");			
//			} else {
//				buffer=buffer.insert(0,"SELECT ID,TYPE FROM INDEXER WHERE (ID,TYPE) in (");
//				
//				buffer.append(") and TOKEN=? ");
//			}
//		}
//		buffer.append(" order by ID desc LIMIT ?,? ");
//		String query=buffer.toString();
//		
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			
//			int i=1;
//			for (String token:tokens){
//				setString(pstm, i++, token, query);
//			}
//			setInt(pstm, i++, first, query);
//			setInt(pstm, i++, number, query);
//	
//			ResultSet rset=executeQuery(pstm, query);
//			
//			List<IndexerResult> list= new ArrayList<IndexerResult>(rset.getFetchSize());
//			
//			while (rset.next()){
//				String id=rset.getString(1);
//				String type=rset.getString(2);
//				
////				TODO el typo tiene que venir del ID
////				String type=rset.getString(2);
//				
//				
//				
//				IndexerResult result=new IndexerResult(type,id);
//				list.add(result);
//			}
//			return list;
//		} catch (SQLException e) {
//			String error = " searchString:'"+StringUtils.join(tokens, ",")+"' first:"+first+" number:"+number;
//			
//			throw new DaoManagerException(error, e);			
//		} catch (DaoManagerException e) {
//			String error = " searchString:'"+StringUtils.join(tokens, ",")+"' first:"+first+" number:"+number;
//			
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}		
//	}
//
//	
//	
//	///<
//	
//	
//
////	// select count(*) from (SELECT ID FROM INDEXER where token='ana' or token='caballero' group by ID,TYPE ) as sq;
////	private static final String COUNT_VAR_START = "select count(*) from (SELECT ID FROM "+TABLE_NAME+" where type=? AND ";
////	// token='ana' or token='caballero'
////	private static final String COUNT_VAR_END = " group by ID ) as sq";
////
////	public int count(HashSet <String>tokens,String type) throws DaoManagerException{
////		Connection connection = getConnection();
////		StringBuilder buffer=new StringBuilder();
////
////		buffer.append(COUNT_VAR_START);
////		for (int i=0;i<tokens.size();i++){
////			if (i != 0) {
////				buffer.append(" or ");			
////			}
////			buffer.append(" token=? ");
////		}
////		buffer.append(COUNT_VAR_END);
////		String query=buffer.toString();
////		
////		
////		try {
////			PreparedStatement pstm = getPrepareStatement(connection,query);
////						
////			int i=1;
////			setString(pstm, i++, type, query);
////			for (String token:tokens){
////				setString(pstm, i++, token, query);
////			}
////	
////			return getIntFromQuery(pstm, query, 0);
////		} catch (DaoManagerException e) {
////			String error = " searchString:'"+StringUtils.join(tokens, ",")+"' type:"+type;
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);
////		} finally {
////			closeConnection(connection);
////		}		
////	}
//
////	private static final String DELETE_FOR_TYPE = "delete  from "+TABLE_NAME+" where type=?";
////	
////	public void deleteForType(String type) throws DaoManagerException {
////		Connection connection = getConnection();
////		String query=DELETE_FOR_TYPE;
////		
////		
////		try {
////			PreparedStatement pstm = getPrepareStatement(connection,query);
////			try {
////				setString(pstm, 1, type, query);
////				
////				
////				executeUpdate(pstm, query);
////			} finally {
////				close(pstm);
////			}
////		} catch (DaoManagerException e) {
////			String error = " type:'"+type;
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);
////		} finally {
////			closeConnection(connection);
////		}				
////	}
//
//	
//
////	private static final String SEARCH_ONE_START_TYPE = "SELECT ID FROM "+TABLE_NAME+" where TYPE=? AND token=? order by ID desc LIMIT ?,?";
////
////	public List<IndexerResult> searchType(String token,String type,int first,int number) throws DaoManagerException{
////		Connection connection = getConnection();
////		String query=SEARCH_ONE_START_TYPE;
////		
////		
////		try {
////			PreparedStatement pstm = getPrepareStatement(connection,query);
////						
////			int i=1;
////			setString(pstm, i++, type, query);
////			setString(pstm, i++, token, query);
////			setInt(pstm, i++, first, query);
////			setInt(pstm, i++, number, query);
////	
////			ResultSet rset=executeQuery(pstm, query);
////	
////			List<IndexerResult> list= new ArrayList<IndexerResult>(rset.getFetchSize());
////			
////			while (rset.next()){
////				String id=rset.getString(1);
////				IndexerResult result=new IndexerResult(type,id);
////				list.add(result);
////			}
////			return list;
////		} catch (SQLException e) {
////			String error = " searchString:'"+token+"' type:"+type+"' first:"+first+"number:"+number;
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);			
////		} catch (DaoManagerException e) {
////			String error = " searchString:'"+token+"' type:"+type+"' first:"+first+"number:"+number;
////			
////			log.error(error, e);
////			throw new DaoManagerException(error, e);
////		} finally {
////			closeConnection(connection);
////		}		
////	}
//
//	private static final String ITERATE_OVER_TOKEN_LIST = "SELECT token,count(*) from  "+TABLE_NAME+" where type=? group by token";
//
//	public interface IteratorOventTokenList{
//		void iterate(String type,String token,int count);
//	}
//	
//	public void iterateOverTokenList(String type,IteratorOventTokenList iterator) throws DaoManagerException{
//		long time=System.currentTimeMillis();
//		Connection connection = getConnection();
//		String query=ITERATE_OVER_TOKEN_LIST;
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			try {
//				setString(pstm, 1, type, query);
//				
//				ResultSet rset=executeQuery(pstm, query);
//				log.error("Time:"+(System.currentTimeMillis()-time));
//				while (rset.next()){
//					String token=rset.getString(1);
//					int number=rset.getInt(2);
//
//					iterator.iterate(type,token,number);
//				}
//			
//				return ;			
//			}finally{
//				close(pstm);
//			}
//		} catch (SQLException e) {
//			String error = " type:'"+type+"'";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);			
//		} catch (DaoManagerException e) {
//			String error = " type:'"+type+"'";
//			
//			log.error(error, e);
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}
//	}
//
//	public void createIndexes() throws DaoManagerException {
//		super.createIndexes(CREATE_INDEX_ARRAY);		
//	}
//	public void dropIndexes() throws DaoManagerException {
//		super.dropIndexes(CREATE_INDEX_ARRAY);		
//	}
//
//
//	private static final IteratorOventTokenList TOKEN_ITERATOR = new IteratorOventTokenList() {
//		
//		@Override
//		public void iterate(String type, String token, int count) {
//			try {
//				TokenDAO.DAO.updateOrInsert(token, type, count);
//			} catch (DaoManagerException e) {
//				log.error("While updating :"+type+" token:"+token+" count:"+count,e);
//			}			
//		}
//	};
//
//	@Override
//	public void createTokenCountInner(String type) throws DaoManagerException {
//		iterateOverTokenList(type, TOKEN_ITERATOR);		
//	}
//
//	@Override
//	public void updateTokenCountInner(String type) throws DaoManagerException {
//		iterateOverTokenList(type, TOKEN_ITERATOR);				
//	}
//
//	private static final String GET_TYPE_TOKEN_NUMBER = "select count(*) from "+TABLE_NAME+" where TOKEN=? AND TYPE=?";
//	
//	public int getNumber(String type, 
//                         String token,
//                         int defaultValue) throws DaoManagerException {
//		Connection connection = getConnection();
//		String query=GET_TYPE_TOKEN_NUMBER;
//	
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			try {
//				int i=1;
//				setString(pstm, i++, token, query);
//				setString(pstm, i++, type, query);
//				
//				return getIntFromQuery(pstm, query, defaultValue);	
//			} finally {
//				close(pstm);
//			}
//		} catch (DaoManagerException e) {
//			String error = "token:'"+token
//				+ "' type:'"+type
//				+"'";
//			
//			throw new DaoManagerException(error, e);
//		} finally {
//			closeConnection(connection);
//		}		
//	}
//
//	private static final String GET_TOKEN_NUMBER = "select count(*) from "+TABLE_NAME+" ";
//	
//	public int getNumber(int defaultValue) throws DaoManagerException {
//		Connection connection = getConnection();
//		String query=GET_TOKEN_NUMBER;
//		
//		try {
//			PreparedStatement pstm = getPrepareStatement(connection,query);
//			try {
//				return getIntFromQuery(pstm,query, defaultValue);	
//			} finally {
//				close(pstm);
//			}			
//		} finally {
//			closeConnection(connection);
//		}		
//	}
//
//
//	@Override
//	public TestResult test() {
//		TestResult ret=new TestResult(getClass());
//		ret.add(super.test());
//
//		if (ret.isOK()){
//			try {
//				getNumber(0);
//			} catch (DaoManagerException e) {
//				ret.addError("Getting the number of elements", e);
//			}
//		}
//		return ret;
//	}
//	
//	public void init(MySQLDataBase dataBase) throws DaoManagerException {
//		setDataBase(dataBase);
//	
//		TestResult ret=test();
//		
//		if (ret.isOK()){
//			return ;
//		} else {
//			// create an test if everything is ok
//			createTable();
//			ret=test();
//			if (ret.isOK()){
//				return;
//			} else {
//				throw new DaoManagerException("Error creating tables..."+ret);
//			}
//		}
//		
//	}
//
//
//}
