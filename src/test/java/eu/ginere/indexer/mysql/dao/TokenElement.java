//package eu.ginere.indexer.mysql.dao;
//
//import java.io.Serializable;
//
//import org.apache.commons.lang.builder.ToStringBuilder;
//import org.apache.commons.lang.builder.ToStringStyle;
//
//public class TokenElement implements Serializable {
//
//	/**
//	 * Serial Id
//	 */
//	private static final long serialVersionUID = "$Version$".hashCode();
//	
//	private final String token;
//	private final String type;
//	private final int count;
//
//	public TokenElement(String tokens, 
//				  String type, 
//				  int nmber) {
//		this.token=tokens;
//		this.type=type;
//		this.count=nmber;
//	}
//
//	public String getToken(){
//		return token;
//	}
//
//	public String getType() {
//		return type;
//	}
//
//	public String toString(){
//		return ToStringBuilder.reflectionToString(this,ToStringStyle.SIMPLE_STYLE);		
//	}
//
//	public int getCount() {
//		return count;
//	}
//}
//
//
