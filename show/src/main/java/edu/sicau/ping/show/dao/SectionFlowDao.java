package edu.sicau.ping.show.dao;

import edu.sicau.ping.show.model.SectionFlow;
import edu.sicau.ping.show.model.SectionFlowKey;
import org.springframework.stereotype.Repository;

@Repository
public interface SectionFlowDao {
    int deleteByPrimaryKey(SectionFlowKey key);

    int insert(SectionFlow record);

    int insertSelective(SectionFlow record);

    SectionFlow selectByPrimaryKey(SectionFlowKey key);

    int updateByPrimaryKeySelective(SectionFlow record);

    int updateByPrimaryKey(SectionFlow record);
}