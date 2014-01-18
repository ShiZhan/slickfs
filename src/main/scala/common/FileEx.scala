package common

object FileEx {
  import java.sql.Date
  import java.text.SimpleDateFormat
  import java.io.{ File, FileInputStream, BufferedInputStream }
  import org.apache.commons.codec.digest.DigestUtils.md5Hex

  private def listAllFiles(file: File): Array[File] = {
    val list = file.listFiles
    if (list == null)
      Array[File]()
    else
      list ++ list.filter(_.isDirectory).flatMap(listAllFiles)
  }

  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  implicit class FileOps(file: File) {
    def flatten =
      if (file.exists)
        if (file.isDirectory) listAllFiles(file) else Array(file)
      else
        Array[File]()

    def checksum =
      try {
        val fIS = new BufferedInputStream(new FileInputStream(file))
        val md5 = md5Hex(fIS)
        fIS.close
        md5
      } catch {
        case e: Exception => ""
      }

    def lastModifiedString = Date.valueOf(dateFormat.format(file.lastModified))
  }
}