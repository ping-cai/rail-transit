package edu.sicau.ping.show.dao;

import edu.sicau.ping.show.model.SectionFlow;
import edu.sicau.ping.show.model.SectionFlowKey;
import edu.sicau.ping.show.vo.SectionFlowGroupByEchrtsVo;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SectionFlowDao {
    int deleteByPrimaryKey(SectionFlowKey key);

    @Select("SELECT * FROM `section_flow` GROUP BY ds")
    List<SectionFlow> selectSectionFlowGroupByTime();


    @Select("SELECT id, granularity, count(flow) as all_flow,ds FROM `section_flow` where ds = #{ds} GROUP BY granularity")
    List<SectionFlowGroupByEchrtsVo> selectSectionFlowGroupByFlow(String ds);


    int insert(SectionFlow record);

    int insertSelective(SectionFlow record);

    SectionFlow selectByPrimaryKey(SectionFlowKey key);

    int updateByPrimaryKeySelective(SectionFlow record);

    int updateByPrimaryKey(SectionFlow record);
}