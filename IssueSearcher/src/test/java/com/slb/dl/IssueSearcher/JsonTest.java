package com.slb.dl.IssueSearcher;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonTest {

	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
	/*	String json1=" {" + 
				"        \"_source\" : {" + 
				"          \"question\" : \"If it is NULL in the source, it becomes  EMPTY STRING in the pond 'For String/Varchar' and 0 if Numeric\"," + 
				"          \"answer\" : \"Null in the source gets changed to '\\\\N' in hive and file using Informatica and the loader automatically loads '\\\\N' as NULL in pond  \"" + 
				"        }" + 
				"      }";*/
		String json1="{" + 
				"  \"_index\" : \"dlissues\"," + 
				"  \"_type\" : \"issue\"," + 
				"  \"_id\" : \"2\"," + 
				"  \"_version\" : 8," + 
				"  \"found\" : true," + 
				"  \"_source\" : {" + 
				"    \"question\" : \"If it is NULL in the source, it becomes  EMPTY STRING in the pond 'For String/Varchar' and 0 if Numeric\"," + 
				"    \"answer\" : \"Null in the source gets changed to '\\\\N' in hive and file using Informatica and the loader automatically loads '\\\\N' as NULL in pond  \"" + 
				"  }" + 
				"}";
		
		//create ObjectMapper instance
				ObjectMapper objectMapper = new ObjectMapper();
				
				//convert json string to object
				HitsSource emp = objectMapper.readValue(json1, HitsSource.class);
				System.out.println(emp.get_source().getAnswer());
	}
}

@JsonIgnoreProperties(ignoreUnknown = true)
class HitsParent {
	Hits hits;

	public Hits getHits() {
		return hits;
	}

	public void setHits(Hits hits) {
		this.hits = hits;
	}
	
}
@JsonIgnoreProperties(ignoreUnknown = true)
class Hits {
	List<HitsSource> hits;

	public List<HitsSource> getHits() {
		return hits;
	}

	public void setHits(List<HitsSource> hits) {
		this.hits = hits;
	}

		
	
}
@JsonIgnoreProperties(ignoreUnknown = true)
class HitsSource {
	Source _source;

	public Source get_source() {
		return _source;
	}

	public void set_source(Source _source) {
		this._source = _source;
	}

	
	
}
@JsonIgnoreProperties(ignoreUnknown = true)
class Source {

	String question;
	String answer;
	public String getQuestion() {
		return question;
	}
	public void setQuestion(String question) {
		this.question = question;
	}
	public String getAnswer() {
		return answer;
	}
	public void setAnswer(String answer) {
		this.answer = answer;
	}
	
}
