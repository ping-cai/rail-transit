package edu.sicau.ping.show.vo;

import lombok.Data;

import java.util.Date;

@Data
public class SectionFlowHistogram {
    private Date ds;
    private String section;
    private Double flow;
}
