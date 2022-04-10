package sink

import afc.AfcRecord
import config.HdfsConf
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.ForeachWriter
import org.slf4j.{Logger, LoggerFactory}
import util.DateUtil

class HdfsAfcSink(hdfsPath: String) extends ForeachWriter[AfcRecord] {
  @transient
  private var output: FSDataOutputStream = _
  @transient
  private var newPath: Path = _
  @transient
  var fileSystem: FileSystem = _
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  override def open(partitionId: Long, epochId: Long): Boolean = {
    val currentDate = DateUtil.getCurrentDate
    val openFile = s"$hdfsPath/trading_date=$currentDate/part-$partitionId"
    val newPath = new Path(openFile)
    this.newPath = newPath
    try {
      fileSystem = HdfsConf.getFileSystem
      if (!fileSystem.exists(newPath)) {
        fileSystem.create(newPath).close()
      } else {
        if (fileSystem.getFileStatus(newPath).getLen >= 120 * 1024 * 1024) {
          val isModify = fileSystem.rename(newPath, new Path(s"$hdfsPath-${System.currentTimeMillis()}"))
          if (isModify) {
            fileSystem.create(newPath).close()
          }
        }
      }
      output = fileSystem.append(newPath)
      output.write(s"${AfcRecord.getHeader}\n".getBytes("UTF-8"))
    } catch {
      case e: Exception =>
        log.error(s"methodName:open:${e.getMessage}")
        close(e)
    }
    true
  }

  override def process(value: AfcRecord): Unit = {
    try {
      if (value == null) {
        return
      }
      if (fileSystem == null) {
        fileSystem = HdfsConf.getFileSystem
      }
      if (output == null) {
        output = fileSystem.append(newPath)
      }
      output.write(s"${value.toString}\n".getBytes("UTF-8"))
    } catch {
      case e: Exception =>
        log.error(s"methodName:process:${e.getMessage}")
        close(e)
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (output != null) {
      output.close()
    }
    if (fileSystem != null) {
      fileSystem.close()
    }
  }
}
