package edu.sicau.ping.show.dao;

import edu.sicau.ping.show.dto.FlowTime;
import edu.sicau.ping.show.model.PartitionKey;
import edu.sicau.ping.show.model.SectionFlow;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.sql.Date;
import java.util.List;

@Repository
public interface SectionFlowDaoExtend {
    List<SectionFlow> selectAll();

    List<SectionFlow> selectByPartition(@Param("ds") Date ds, @Param("granularity") Integer granularity);

    List<SectionFlow> selectBySectionIdList(@Param("partitionKey") PartitionKey partitionKey,
                                            @Param("sectionIdList") List<Integer> sectionIdList);

    List<FlowTime> selectFlowTime(@Param("ds") Date ds, @Param("granularity") Integer granularity,
                                  @Param("sectionId") Integer sectionId);

    List<SectionFlow> selectLately(@Param("lately") Integer lately, @Param("sectionIdList") List<Integer> sectionIdList);
}