package com.slb.dl.IssueSearcher.model;

import org.hibernate.validator.constraints.NotBlank;

public class SearchCriteria {

    String issue;

    public String getIssue() {
        return issue;
    }

    public void setIssue(String issue) {
        this.issue = issue;
    }
}
