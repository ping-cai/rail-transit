package edu.sicau.ping.show.service;

import edu.sicau.ping.show.dao.SectionFlowDaoExtend;
import edu.sicau.ping.show.dao.SectionInfoDaoExtend;
import edu.sicau.ping.show.dao.StationInfoDaoExtend;
import edu.sicau.ping.show.model.SectionFlow;
import edu.sicau.ping.show.model.SectionInfo;
import edu.sicau.ping.show.model.StationInfo;
import edu.sicau.ping.show.vo.SectionFlowHistogram;
import edu.sicau.ping.show.vo.SectionFlowVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

@Service
public class SectionFlowService {
    public static final int SECTION_NUM = 10;
    private final SectionFlowDaoExtend sectionFlowDaoExtend;
    private final SectionInfoDaoExtend sectionInfoDaoExtend;
    private final StationInfoDaoExtend stationInfoDaoExtend;
    public static List<SectionInfo> sectionInfoList;
    public static final Random RANDOM = new Random();

    @Autowired(required = false)
    public SectionFlowService(SectionFlowDaoExtend sectionFlowDaoExtend, SectionInfoDaoExtend sectionInfoDaoExtend, StationInfoDaoExtend stationInfoDaoExtend) {
        this.sectionFlowDaoExtend = sectionFlowDaoExtend;
        this.sectionInfoDaoExtend = sectionInfoDaoExtend;
        this.stationInfoDaoExtend = stationInfoDaoExtend;
        sectionInfoList = sectionInfoDaoExtend.selectAll();
    }

    public List<SectionFlowHistogram> histogramList(int lately) {
        List<SectionInfo> sectionInfoList = SectionFlowService.sectionInfoList;
        ArrayList<Integer> sectionIdList = new ArrayList<>();
        for (int i = 0; i < SECTION_NUM; i++) {
            int index = RANDOM.nextInt(sectionInfoList.size());
            sectionIdList.add(sectionInfoList.get(index).getId());
        }
        List<SectionFlow> sectionFlows = sectionFlowDaoExtend.selectLately(lately, sectionIdList);
        List<SectionInfo> sectionInfos = sectionInfoDaoExtend.selectByList(sectionIdList);
        Map<Integer, int[]> sectionIdMap = sectionInfos
                .stream().collect(Collectors.toMap(SectionInfo::getId, x -> {
                    Integer stationIn = x.getStation_in();
                    Integer stationOut = x.getStation_out();
                    return new int[]{stationIn, stationOut};
                }));
        List<StationInfo> stationInfos = stationInfoDaoExtend.listAll();
        Map<Integer, String> stationIdMap = stationInfos
                .stream().collect(Collectors.toMap(StationInfo::getId, StationInfo::getName));
        return sectionFlows.stream().map(x -> {
            Double flow = x.getFlow();
            java.util.Date ds = x.getDs();
            Integer sectionId = x.getSection_id();
            int[] stationArray = sectionIdMap.get(sectionId);
            int stationIn = stationArray[0];
            int stationOut = stationArray[1];
            SectionFlowHistogram sectionFlowHistogram = new SectionFlowHistogram();
            sectionFlowHistogram.setFlow(flow);
            sectionFlowHistogram.setDs(ds);
            String section = String.format("%s-%s", stationIdMap.get(stationIn), stationIdMap.get(stationOut));
            sectionFlowHistogram.setSection(section);
            return sectionFlowHistogram;
        }).collect(Collectors.toList());
    }

    public List<SectionFlowVo> sectionFlowVoList(Date ds) {
        List<SectionFlow> sectionFlows = sectionFlowDaoExtend.selectByPartition(ds, 15);
        List<Integer> sectionIdList = sectionFlows.stream().map(SectionFlow::getSection_id).collect(Collectors.toList());
        List<SectionInfo> sectionInfos = sectionInfoDaoExtend.selectByList(sectionIdList);
        Map<Integer, int[]> sectionIdMap = sectionInfos
                .stream().collect(Collectors.toMap(SectionInfo::getId, x -> {
                    Integer stationIn = x.getStation_in();
                    Integer stationOut = x.getStation_out();
                    return new int[]{stationIn, stationOut};
                }));
        List<StationInfo> stationInfos = stationInfoDaoExtend.listAll();
        Map<Integer, String> stationIdMap = stationInfos
                .stream().collect(Collectors.toMap(StationInfo::getId, StationInfo::getName));
        return sectionFlows.stream().map(x -> {
            Double flow = x.getFlow();
            java.util.Date startTime = x.getStart_time();
            Integer sectionId = x.getSection_id();
            int[] stationArray = sectionIdMap.get(sectionId);
            int stationIn = stationArray[0];
            int stationOut = stationArray[1];
            SectionFlowVo sectionFlowVo = new SectionFlowVo();
            String section = String.format("%s-%s", stationIdMap.get(stationIn), stationIdMap.get(stationOut));
            sectionFlowVo.setSectionName(section);
            sectionFlowVo.setLastTime(startTime);
            sectionFlowVo.setFlow(flow);
            return sectionFlowVo;
        }).collect(Collectors.toList());
    }
}
