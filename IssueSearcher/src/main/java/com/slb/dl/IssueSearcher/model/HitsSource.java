package com.slb.dl.IssueSearcher.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HitsSource {
	Source _source;
	String _id;
	
	public String get_id() {
		return _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}

	public Source get_source() {
		return _source;
	}

	public void set_source(Source _source) {
		this._source = _source;
	}

	
	
}
