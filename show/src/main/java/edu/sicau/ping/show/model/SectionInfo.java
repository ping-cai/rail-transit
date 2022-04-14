package edu.sicau.ping.show.model;

import java.io.Serializable;

/**
 * section_info
 * @author 
 */
public class SectionInfo implements Serializable {
    private Integer id;

    private Integer station_in;

    private Integer station_out;

    private Integer section_direction;

    private String line_name;

    private Double length;

    private Integer travel_time;

    private static final long serialVersionUID = 1L;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getStation_in() {
        return station_in;
    }

    public void setStation_in(Integer station_in) {
        this.station_in = station_in;
    }

    public Integer getStation_out() {
        return station_out;
    }

    public void setStation_out(Integer station_out) {
        this.station_out = station_out;
    }

    public Integer getSection_direction() {
        return section_direction;
    }

    public void setSection_direction(Integer section_direction) {
        this.section_direction = section_direction;
    }

    public String getLine_name() {
        return line_name;
    }

    public void setLine_name(String line_name) {
        this.line_name = line_name;
    }

    public Double getLength() {
        return length;
    }

    public void setLength(Double length) {
        this.length = length;
    }

    public Integer getTravel_time() {
        return travel_time;
    }

    public void setTravel_time(Integer travel_time) {
        this.travel_time = travel_time;
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
        SectionInfo other = (SectionInfo) that;
        return (this.getId() == null ? other.getId() == null : this.getId().equals(other.getId()))
            && (this.getStation_in() == null ? other.getStation_in() == null : this.getStation_in().equals(other.getStation_in()))
            && (this.getStation_out() == null ? other.getStation_out() == null : this.getStation_out().equals(other.getStation_out()))
            && (this.getSection_direction() == null ? other.getSection_direction() == null : this.getSection_direction().equals(other.getSection_direction()))
            && (this.getLine_name() == null ? other.getLine_name() == null : this.getLine_name().equals(other.getLine_name()))
            && (this.getLength() == null ? other.getLength() == null : this.getLength().equals(other.getLength()))
            && (this.getTravel_time() == null ? other.getTravel_time() == null : this.getTravel_time().equals(other.getTravel_time()));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getId() == null) ? 0 : getId().hashCode());
        result = prime * result + ((getStation_in() == null) ? 0 : getStation_in().hashCode());
        result = prime * result + ((getStation_out() == null) ? 0 : getStation_out().hashCode());
        result = prime * result + ((getSection_direction() == null) ? 0 : getSection_direction().hashCode());
        result = prime * result + ((getLine_name() == null) ? 0 : getLine_name().hashCode());
        result = prime * result + ((getLength() == null) ? 0 : getLength().hashCode());
        result = prime * result + ((getTravel_time() == null) ? 0 : getTravel_time().hashCode());
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(" [");
        sb.append("Hash = ").append(hashCode());
        sb.append(", id=").append(id);
        sb.append(", station_in=").append(station_in);
        sb.append(", station_out=").append(station_out);
        sb.append(", section_direction=").append(section_direction);
        sb.append(", line_name=").append(line_name);
        sb.append(", length=").append(length);
        sb.append(", travel_time=").append(travel_time);
        sb.append(", serialVersionUID=").append(serialVersionUID);
        sb.append("]");
        return sb.toString();
    }
}