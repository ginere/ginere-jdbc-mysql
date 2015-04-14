package eu.ginere.indexer.mysql.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * This represent the result of a search
 * 
 * @author mendang
 *
 */
public class IndexerResult implements Serializable {

	/**
	 * Serial Id
	 */
	private static final long serialVersionUID = "$Version$".hashCode();

	public static final List<IndexerResult> EMPTY_LIST = new ArrayList<IndexerResult>(0);
	
	public final String type;
	public final String id;

	public IndexerResult(String type, 
				  String id) {
		this.type=type;
		this.id=id;
	}

	public String getType() {
		return type;
	}

	public String getId() {
		return id;
	}
	
	public String toString(){
		return ToStringBuilder.reflectionToString(this,ToStringStyle.SIMPLE_STYLE);		
	}
}


