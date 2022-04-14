package edu.sicau.ping.show.dao;

import edu.sicau.ping.show.model.SectionInfo;

public interface SectionInfoDao {
    int deleteByPrimaryKey(Integer id);

    int insert(SectionInfo record);

    int insertSelective(SectionInfo record);

    SectionInfo selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(SectionInfo record);

    int updateByPrimaryKey(SectionInfo record);
}