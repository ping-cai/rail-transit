import lombok.Data;

import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * AFC记录对象
 */
@Data
public class AFCRecord {
    private static final Random random = new Random();
    public static final String TICKET_EXAMPLE = "00004000637665361410";
    public static final long TICKET_PREFIX = 4000000000000000L;
    public static final Map<String, AFCRecord> TICKET_MAP = new HashMap<String, AFCRecord>();
    public static final int TICKET_NUM = 5000000;
    public static final String STATION_IN = "21";
    public static final String STATION_OUT = "22";
    public static final List<String> STATION_LIST = new ArrayList<String>();
    public static final List<Integer> JOINT_TRANSACTION = Arrays.asList(0, 170);
    public static final int EQUIPMENT_NUMBER_EXAMPLE = 3000000;
    public static final String QUERY_STATION_SQL = "SELECT station_id FROM afc_station_info";
    /**
     * 预留
     */
    private int reserve;
    /**
     * 发送时间
     */
    private Timestamp sendingTime;
    /**
     * 记录数量
     */
    private int recordNumber;
    /**
     * 记录序号
     */
    private int recordSeq;
    /**
     * 票卡号
     */
    private String ticketId;
    /**
     * 交易时间
     */
    private Timestamp tradingTime;
    /**
     * 票卡种类
     */
    private String cardType;
    /**
     * 交易事件
     */
    private String transactionEvent;
    /**
     * 交易车站编号
     */
    private String stationId;
    /**
     * 是否联程交易
     */
    private int jointTransaction;
    /**
     * 交易设备编号
     */
    private String equipmentNumber;

    public static AFCRecord getOneRecord() {
        AFCRecord afcRecord = new AFCRecord();
        afcRecord.setReserve(0);
        Timestamp randomSendTime = TimeUtil.getRandomTimestamp();
        afcRecord.setSendingTime(randomSendTime);
        afcRecord.setRecordNumber(1000);
        afcRecord.setRecordSeq(random.nextInt(1000));
        String ticketSuffix = String.valueOf(TICKET_PREFIX + random.nextInt(TICKET_NUM));
        afcRecord.setTicketId(ticketSuffix);
        Timestamp randomTradingTime = new Timestamp(System.currentTimeMillis());
        randomTradingTime.setTime(randomSendTime.getTime() - random.nextInt(TimeUtil.HOUR * 2));
        afcRecord.setTradingTime(randomTradingTime);
        afcRecord.setCardType("77");
        afcRecord.setStationId(STATION_LIST.get(random.nextInt(STATION_LIST.size())));
        afcRecord.setJointTransaction(JOINT_TRANSACTION.get(random.nextInt(2)));
        afcRecord.setEquipmentNumber(String.valueOf(EQUIPMENT_NUMBER_EXAMPLE + random.nextInt(100000)));
        if (TICKET_MAP.containsKey(ticketSuffix)) {
            AFCRecord afcRecordIn = TICKET_MAP.get(ticketSuffix);
            Timestamp tradingTime = afcRecordIn.getTradingTime();
            randomTradingTime.setTime(tradingTime.getTime() + (random.nextInt(TimeUtil.HOUR * 3)));
            afcRecord.setTradingTime(randomTradingTime);
            afcRecord.setTransactionEvent(STATION_OUT);
            Timestamp sendingTime = afcRecord.getSendingTime();
            sendingTime.setTime(randomTradingTime.getTime() + random.nextInt(TimeUtil.HOUR * 2));
            afcRecord.setSendingTime(sendingTime);
            TICKET_MAP.remove(ticketSuffix);
        } else {
            afcRecord.setTransactionEvent(STATION_IN);
            TICKET_MAP.put(ticketSuffix, afcRecord);
        }
        return afcRecord;
    }

    public static void setStationList() throws SQLException, IOException, ClassNotFoundException {
        Connection connection = JdbcUtil.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(QUERY_STATION_SQL);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            STATION_LIST.add(resultSet.getString("station_id"));
        }
    }
}
