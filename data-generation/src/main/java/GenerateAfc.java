import afc.AfcRecord;
import lombok.extern.slf4j.Slf4j;
import util.JdbcUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

/**
 * 生成AFC数据到数据库中
 */
@Slf4j
public class GenerateAfc {
    /**
     * 随机生成工具
     */
    private static final Random random = new Random();
    /**
     * 批量插入语句
     */
    public static final String SQL =
            "INSERT INTO afc_record(reserve,sending_time,record_number,record_seq,ticket_id," +
                    "trading_time,card_type,transaction_event,station_id," +
                    "joint_transaction,equipment_number) VALUES(?,?,?,?,?,?,?,?,?,?,?)";

    /**
     * 批量生成记录，并插入数据库中
     *
     * @throws SQLException
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void generate(int generateNum) throws SQLException, IOException, ClassNotFoundException, InterruptedException {
        Connection connection = JdbcUtil.getConnection();
        connection.setAutoCommit(false);
        PreparedStatement preparedStatement = connection.prepareStatement(SQL);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        AfcProducer afcProducer = new AfcProducer();
        while (true) {
            LocalDateTime dateTime = LocalDateTime.now();
            String date = dateTime.format(dateTimeFormatter);
            if ((dateTime.getHour() < 6)) {
                Thread.sleep(random.nextInt(2000 * 60));
                continue;
            }
            int num = random.nextInt(generateNum);
            log.info("时间点:{} 开始加载AFC数据! 加载数量为:{} ", date, num);
            for (int i = 0; i < num; i++) {
                AfcRecord oneRecord = AfcRecord.getOneRecord();
                afcProducer.sendAfc(oneRecord);
                setRecord(preparedStatement, oneRecord);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
            log.info("时间点:{} 批量插入AFC数据成功!", date);
            Thread.sleep(random.nextInt(2000));
        }
    }

    /**
     * 设置记录在SQL中的位置
     *
     * @param preparedStatement
     * @throws SQLException
     */
    private static void setRecord(PreparedStatement preparedStatement, AfcRecord oneRecord) throws SQLException {
        preparedStatement.setInt(1, oneRecord.getReserve());
        preparedStatement.setTimestamp(2, oneRecord.getSendingTime());
        preparedStatement.setInt(3, oneRecord.getRecordNumber());
        preparedStatement.setInt(4, oneRecord.getRecordSeq());
        preparedStatement.setString(5, oneRecord.getTicketId());
        preparedStatement.setTimestamp(6, oneRecord.getTradingTime());
        preparedStatement.setString(7, oneRecord.getCardType());
        preparedStatement.setString(8, oneRecord.getTransactionEvent());
        preparedStatement.setString(9, oneRecord.getStationId());
        preparedStatement.setInt(10, oneRecord.getJointTransaction());
        preparedStatement.setString(11, oneRecord.getEquipmentNumber());
    }

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, InterruptedException {
        GenerateAfc generateAfc = new GenerateAfc();
        generateAfc.generate(100);
    }
}
