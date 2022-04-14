package edu.sicau.ping.show.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

@Data
public class PartitionKey implements Serializable {
    /**
     * 分区键
     */
    @JsonFormat(locale = "zh", timezone = "GMT+8", pattern = "yyyy-MM-dd")
    private Date ds;
    /**
     * 时间粒度
     */
    private Integer granularity;
}
