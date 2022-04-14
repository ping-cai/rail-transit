package edu.sicau.ping.show.vo;

import edu.sicau.ping.show.model.PartitionKey;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class SectionKeyVo implements Serializable {
    private PartitionKey partitionKey;
    private List<Integer> sectionIdList;
}
