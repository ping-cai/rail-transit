package edu.sicau.ping.show.vo;

import edu.sicau.ping.show.util.ColorUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = "sectionName")
public class SectionWithColorVo {
    //    地区名称
    private String sectionName;
    //   生成随机颜色
    private String color = ColorUtils.generateRandomColor();
}
