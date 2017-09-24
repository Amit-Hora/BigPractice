package com.slb.dl.IssueSearcher.model;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class Response {
	Hits hits;

	public Hits getHits() {
		return hits;
	}

	public void setHits(Hits hits) {
		this.hits = hits;
	}
	
	public List<String> getQuestions(){
		List<String> questions=new ArrayList<String>();
		if(this.getHits()!=null)
		for(HitsSource hs:this.getHits().getHits()){
			String question=hs.get_source().getQuestion();
		/*	if(question.length()>60){
				question=question.substring(0, 60);
			}*/
			questions.add(question+"$%"+hs.get_id());
		}
		return questions;
		
	}
	
	public List<String> getAnswers(){
		List<String> answers=new ArrayList<String>();
		for(HitsSource hs:this.getHits().getHits()){
		
			answers.add(hs.get_source().getAnswer());
		}
		return answers;
		
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


