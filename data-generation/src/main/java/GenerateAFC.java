import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Random;

/**
 * 生成AFC数据到数据库中
 */
@Slf4j
public class GenerateAFC {
    private static final Random random = new Random();
    public static final String SQL =
            "INSERT INTO afc_record(reserve,sending_time,record_number,record_seq,ticket_id," +
                    "trading_time,card_type,transaction_event,station_id," +
                    "joint_transaction,equipment_number) VALUES(?,?,?,?,?,?,?,?,?,?,?)";

    public static void generate() throws SQLException, IOException, ClassNotFoundException, InterruptedException {
        Connection connection = JdbcUtil.getConnection();
        connection.setAutoCommit(false);
        PreparedStatement preparedStatement = connection.prepareStatement(SQL);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        while (true) {
            LocalDateTime dateTime = LocalDateTime.now();
            String date = dateTime.format(dateTimeFormatter);
            if ((dateTime.getHour() < 6)) {
                Thread.sleep(random.nextInt(2000 * 60));
                continue;
            }
            int num = random.nextInt(100);
            log.info("时间点:{} 开始加载AFC数据! 加载数量为:{} ", date, num);
            for (int i = 0; i < num; i++) {
                setRecord(preparedStatement);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
            log.info("时间点:{} 批量插入AFC数据成功!", date);
            Thread.sleep(random.nextInt(2000));
        }
    }

    private static void setRecord(PreparedStatement preparedStatement) throws SQLException {
        AFCRecord oneRecord = AFCRecord.getOneRecord();
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
        AFCRecord.setStationList();
        generate();
    }
}
