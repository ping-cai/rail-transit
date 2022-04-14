package edu.sicau.ping.show.dao;

import edu.sicau.ping.show.model.SectionInfo;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface SectionInfoDaoExtend {
    List<SectionInfo> selectByList(@Param("sectionIdList") List<Integer> sectionIdList);

    List<SectionInfo> selectAll();
}