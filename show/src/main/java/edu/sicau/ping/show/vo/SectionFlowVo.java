package edu.sicau.ping.show.vo;

import lombok.Data;

import java.util.Date;

@Data
public class SectionFlowVo {
    // 地区名称
    private String sectionName;
    // 客流量
    private Double flow;
    // 最新时间
    private Date lastTime;
}
