package edu.sicau.ping.show.controller;

import edu.sicau.ping.show.dao.SectionFlowDao;
import edu.sicau.ping.show.dao.SectionFlowDaoExtend;
import edu.sicau.ping.show.dto.FlowTime;
import edu.sicau.ping.show.model.PartitionKey;
import edu.sicau.ping.show.model.SectionFlow;
import edu.sicau.ping.show.model.SectionFlowKey;
import edu.sicau.ping.show.service.SectionFlowService;
import edu.sicau.ping.show.util.TimeUtil;
import edu.sicau.ping.show.vo.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/flow")
public class SectionFlowController {
    private final SectionFlowDaoExtend sectionFlowDaoExtend;
    private final SectionFlowDao sectionFlowDao;
    private final SectionFlowService sectionFlowService;

    @Autowired(required = false)
    public SectionFlowController(SectionFlowDaoExtend sectionFlowDaoExtend, SectionFlowDao sectionFlowDao, SectionFlowService sectionFlowService) {
        this.sectionFlowDaoExtend = sectionFlowDaoExtend;
        this.sectionFlowDao = sectionFlowDao;
        this.sectionFlowService = sectionFlowService;
    }

    @GetMapping("/section/all")
    public List<SectionFlow> getSection() {
        return sectionFlowDaoExtend.selectAll();
    }

    @GetMapping("/section/get/list/{date}")
    public List<SectionFlow> getSection(@PathVariable String date) {
        Date ds = Date.valueOf(date);
        return sectionFlowDaoExtend.selectByPartition(ds, 15);
    }

    @GetMapping("/section/get/list/{date}/{granularity}")
    public List<SectionFlow> getSection(@PathVariable String date, @PathVariable Integer granularity) {
        Date ds = Date.valueOf(date);
        return sectionFlowDaoExtend.selectByPartition(ds, granularity);
    }

    @GetMapping("/section/get/{id}/{date}")
    public SectionFlow getSection(@PathVariable Long id, @PathVariable String date) {
        Date ds = Date.valueOf(date);
        SectionFlowKey sectionFlowKey = new SectionFlowKey();
        sectionFlowKey.setId(id);
        sectionFlowKey.setDs(ds);
        return sectionFlowDao.selectByPrimaryKey(sectionFlowKey);
    }

    @PostMapping("/section/get/list")
    public List<SectionFlow> getSection(@RequestBody SectionKeyVo sectionKeyVo) {
        PartitionKey partitionKey = sectionKeyVo.getPartitionKey();
        List<Integer> sectionIdList = sectionKeyVo.getSectionIdList();
        return sectionFlowDaoExtend.selectBySectionIdList(partitionKey, sectionIdList);
    }

    @PostMapping("/section/get/timeInterval")
    @CrossOrigin
    public List<String> getTimeInterval(@RequestParam("granularity") Integer granularity) {
        return TimeUtil.getTimeInterval(granularity);
    }

    @PostMapping("/section/get/flow_time")
    @CrossOrigin
    public List<FlowTime> getFlowTime(@RequestParam("date") String date, @RequestParam("granularity") Integer granularity) {
        Date ds = Date.valueOf(date);
        List<FlowTime> flowTimes = sectionFlowDaoExtend.selectFlowTime(ds, granularity, 455);
        return flowTimes;
    }

    @PostMapping("/section/get/histogram")
    @CrossOrigin
    public Map<String, Object> getHistogram(@RequestParam("lately") Integer lately) {
        List<SectionFlowHistogram> sectionFlowHistograms = sectionFlowService.histogramList(lately);
        HashMap<java.util.Date, Integer> dateMap = new HashMap<>();
        HashMap<String, Integer> sectionMap = new HashMap<>();
        for (SectionFlowHistogram sectionFlowHistogram : sectionFlowHistograms) {
            java.util.Date ds = sectionFlowHistogram.getDs();
            dateMap.put(ds, dateMap.getOrDefault(ds, dateMap.size() + 1));
            String section = sectionFlowHistogram.getSection();
            sectionMap.put(section, sectionMap.getOrDefault(section, sectionMap.size()));
        }
        Object[][] body = new Object[sectionMap.size()][dateMap.size() + 1];
        for (SectionFlowHistogram sectionFlowHistogram : sectionFlowHistograms) {
            java.util.Date ds = sectionFlowHistogram.getDs();
            String section = sectionFlowHistogram.getSection();
            Integer row = sectionMap.get(section);
            Integer column = dateMap.get(ds);
            body[row][column] = sectionFlowHistogram.getFlow();
        }
        for (String section : sectionMap.keySet()) {
            Integer index = sectionMap.get(section);
            body[index][0] = section;
        }
        HashMap<String, Object> res = new HashMap<>();
        res.put("header", dateMap.keySet());
        res.put("body", body);
        return res;
    }

    @PostMapping("/section/get/dynamic_order")
    @CrossOrigin
    public Map<String, Object> getDynamicOrder(@RequestParam("date") String date) {
        Date ds = Date.valueOf(date);
        List<SectionFlowVo> sectionFlowVoList = sectionFlowService.sectionFlowVoList(ds);
        Set<SectionWithColorVo> sectionWithColorVoSet = sectionFlowVoList.stream().map(x -> {
            String sectionName = x.getSectionName();
            SectionWithColorVo sectionWithColorVo = new SectionWithColorVo();
            sectionWithColorVo.setSectionName(sectionName);
            return sectionWithColorVo;
        }).collect(Collectors.toSet());
        ArrayList<SectionFlowVo> distinctSectionFlowVoList = sectionFlowVoList.stream().collect(Collectors.collectingAndThen(Collectors.toCollection(() ->
                new TreeSet<>(Comparator.comparing(x -> x.getLastTime() + ";" + x.getSectionName()))), ArrayList::new));
        HashMap<String, Object> res = new HashMap<>();
        res.put("sectionFlowVoList", distinctSectionFlowVoList);
        res.put("sectionWithColorVoSet", sectionWithColorVoSet);
        return res;
    }


    @GetMapping("/test")
    public ModelAndView test(){
        ModelAndView modelAndView = new ModelAndView();
        List<SectionFlow> sectionFlowList = sectionFlowDao.selectSectionFlowGroupByTime();
        List<SectionFlowGroupByEchrtsVo> sectionFlows = sectionFlowDao.selectSectionFlowGroupByFlow("2022-04-02");
        List<java.util.Date> time = sectionFlowList.stream().map(SectionFlow::getDs).collect(Collectors.toList());

        List<Integer> type = new ArrayList<>();
        List<Double> flow = new ArrayList<>();


        modelAndView.addObject("time", time);
        modelAndView.addObject("type", type);
        modelAndView.addObject("flow", flow);
        modelAndView.setViewName("/test");

        return modelAndView;
    }
}
