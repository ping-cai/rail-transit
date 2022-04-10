package afc

import java.sql.Timestamp

/**
  *
  * @param reserve          预留
  * @param sendingTime      发送时间
  * @param recordNumber     记录数量
  * @param recordSeq        记录序号
  * @param ticketId         票卡号
  * @param tradingTime      交易时间
  * @param cardType         票卡种类
  * @param transactionEvent 交易事件
  * @param stationId        交易车站编号
  * @param jointTransaction 是否联程交易
  * @param equipmentNumber  交易设备编号
  */
case class AfcRecord(reserve: Int, sendingTime: Timestamp, recordNumber: Int,
                     recordSeq: Int, ticketId: String, tradingTime: Timestamp,
                     cardType: String, transactionEvent: String, stationId: String,
                     jointTransaction: Int, equipmentNumber: String) {

  override def toString: String = {
    s"$reserve,$sendingTime,$recordNumber,$recordSeq,$ticketId,$tradingTime,$cardType,$transactionEvent,$stationId,$jointTransaction,$equipmentNumber"
  }
}

object AfcRecord {
  def getHeader: String = {
    s"reserve,sending_time,record_number,record_seq,ticket_id,trading_time,card_type,transaction_event,station_id,joint_transaction,equipment_number"
  }

  def main(args: Array[String]): Unit = {

  }
}
