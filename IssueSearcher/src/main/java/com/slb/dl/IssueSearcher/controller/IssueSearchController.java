package com.slb.dl.IssueSearcher.controller;

import java.io.IOException;

import javax.validation.Valid;

import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slb.dl.IssueSearcher.model.HitsSource;
import com.slb.dl.IssueSearcher.model.Response;
import com.slb.dl.IssueSearcher.model.SearchCriteria;

@Controller
public class IssueSearchController {

	@RequestMapping(method = RequestMethod.GET, value = "/test")
	public @ResponseBody String getDiskUsage() {
		return "String";
	}
	
	
	@PostMapping("/answer")
	public ResponseEntity<?> getAnswers(@Valid @RequestBody SearchCriteria search, Errors errors) throws JsonParseException, JsonMappingException, IOException {

		
	
		ObjectMapper mapper = new ObjectMapper();
		String json = "";
	

		RestTemplate restTemplate = new RestTemplate();
		String url = "http://localhost:9200/dlissues/issue/"+search.getIssue()+"?pretty";
		
	
		String res=restTemplate.getForObject(url, String.class);
		
		ObjectMapper objectMapper = new ObjectMapper();
		
		//convert json string to object
		HitsSource questionsResponse = objectMapper.readValue(res, HitsSource.class);
	
		try {
			json = mapper.writeValueAsString(questionsResponse.get_source().getAnswer());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return ResponseEntity.ok(json);

	}
	
	

	@PostMapping("/api/search")
	public ResponseEntity<?> getSearchResultViaAjax(@Valid @RequestBody SearchCriteria search, Errors errors) throws IOException {
		if(!search.getIssue().isEmpty() && !search.getIssue().equalsIgnoreCase(" ") ){
		
			
		ObjectMapper mapper = new ObjectMapper();
		String json = "";
	

		RestTemplate restTemplate = new RestTemplate();
		String url = "http://localhost:9200/dlissues/issue/_search?pretty&filter_path=hits.hits._source,hits.hits._id";
		String query="{" + 
				"\"_source\": [\"question\"]," + 
				"    \"query\": {\r\n" + 
				"        \"match\": {\r\n" + 
				"            \"question\": \""+search.getIssue()+"\"" + 
				"        }" + 
				"    }" + 
				"}";
		HttpEntity<String> httpEntity = new HttpEntity<String>(query);
		String res=restTemplate.postForObject(url, httpEntity, String.class);
		
		ObjectMapper objectMapper = new ObjectMapper();
		
		//convert json string to object
		Response questionsResponse = objectMapper.readValue(res, Response.class);
	
		try {
			json = mapper.writeValueAsString(questionsResponse.getQuestions());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return ResponseEntity.ok(json);
	}
	
		return ResponseEntity.ok("");
	}
}