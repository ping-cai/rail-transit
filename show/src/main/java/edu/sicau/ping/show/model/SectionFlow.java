package edu.sicau.ping.show.model;

import java.io.Serializable;
import java.util.Date;

/**
 * section_flow
 *
 * @author
 */
public class SectionFlow extends SectionFlowKey implements Serializable {
    /**
     * 区间id
     */
    private Integer section_id;

    /**
     * 区间开始时间
     */
    private Date start_time;

    /**
     * 区间结束时间
     */
    private Date end_time;

    /**
     * 时间粒度
     */
    private Integer granularity;

    /**
     * 流量
     */
    private Double flow;

    /**
     * 拥挤度
     */
    private Double crowd_degree;

    private static final long serialVersionUID = 1L;

    public Integer getSection_id() {
        return section_id;
    }

    public void setSection_id(Integer section_id) {
        this.section_id = section_id;
    }

    public Date getStart_time() {
        return start_time;
    }

    public void setStart_time(Date start_time) {
        this.start_time = start_time;
    }

    public Date getEnd_time() {
        return end_time;
    }

    public void setEnd_time(Date end_time) {
        this.end_time = end_time;
    }

    public Integer getGranularity() {
        return granularity;
    }

    public void setGranularity(Integer granularity) {
        this.granularity = granularity;
    }

    public Double getFlow() {
        return flow;
    }

    public void setFlow(Double flow) {
        this.flow = flow;
    }

    public Double getCrowd_degree() {
        return crowd_degree;
    }

    public void setCrowd_degree(Double crowd_degree) {
        this.crowd_degree = crowd_degree;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that == null) {
            return false;
        }
        if (getClass() != that.getClass()) {
            return false;
        }
        SectionFlow other = (SectionFlow) that;
        return (this.getId() == null ? other.getId() == null : this.getId().equals(other.getId()))
                && (this.getDs() == null ? other.getDs() == null : this.getDs().equals(other.getDs()))
                && (this.getSection_id() == null ? other.getSection_id() == null : this.getSection_id().equals(other.getSection_id()))
                && (this.getStart_time() == null ? other.getStart_time() == null : this.getStart_time().equals(other.getStart_time()))
                && (this.getEnd_time() == null ? other.getEnd_time() == null : this.getEnd_time().equals(other.getEnd_time()))
                && (this.getGranularity() == null ? other.getGranularity() == null : this.getGranularity().equals(other.getGranularity()))
                && (this.getFlow() == null ? other.getFlow() == null : this.getFlow().equals(other.getFlow()))
                && (this.getCrowd_degree() == null ? other.getCrowd_degree() == null : this.getCrowd_degree().equals(other.getCrowd_degree()));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
        result = prime * result + ((getDs() == null) ? 0 : getDs().hashCode());
        result = prime * result + ((getSection_id() == null) ? 0 : getSection_id().hashCode());
        result = prime * result + ((getStart_time() == null) ? 0 : getStart_time().hashCode());
        result = prime * result + ((getEnd_time() == null) ? 0 : getEnd_time().hashCode());
        result = prime * result + ((getGranularity() == null) ? 0 : getGranularity().hashCode());
        result = prime * result + ((getFlow() == null) ? 0 : getFlow().hashCode());
        result = prime * result + ((getCrowd_degree() == null) ? 0 : getCrowd_degree().hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [");
        sb.append("Hash = ").append(hashCode());
        sb.append(", section_id=").append(section_id);
        sb.append(", start_time=").append(start_time);
        sb.append(", end_time=").append(end_time);
        sb.append(", granularity=").append(granularity);
        sb.append(", flow=").append(flow);
        sb.append(", crowd_degree=").append(crowd_degree);
        sb.append(", serialVersionUID=").append(serialVersionUID);
        sb.append("]");
        return sb.toString();
    }
}