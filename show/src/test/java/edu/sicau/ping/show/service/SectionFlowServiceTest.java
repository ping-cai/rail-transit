package edu.sicau.ping.show.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class SectionFlowServiceTest {
    @Autowired
    private SectionFlowService sectionFlowService;
    @Test
    public void histogramList() {
        System.out.println(sectionFlowService.histogramList(14));
    }
}
