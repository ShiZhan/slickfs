package database

object FSDatabase {
  import java.io.{ File, FileInputStream, BufferedInputStream }
  import java.sql.{ Date, ResultSet }
  import java.text.SimpleDateFormat
  import scala.io.Source
  import scala.slick.driver.H2Driver.simple._
  import scala.util.Properties.{ envOrElse, userDir }
  import org.apache.commons.codec.digest.DigestUtils.md5Hex

  class DirectoryTable(tag: Tag)
    extends Table[(String, Long, Date, Boolean, Boolean, Boolean, Boolean)](tag, "DIRECTORY") {
    def name = column[String]("NAME", O.PrimaryKey)
    def fileSize = column[Long]("FILESIZE")
    def lastMod = column[Date]("LASTMODIFIED")
    def canRead = column[Boolean]("R")
    def canWrite = column[Boolean]("W")
    def canExecute = column[Boolean]("X")
    def isDirectory = column[Boolean]("D")
    def * = (name, fileSize, lastMod, canRead, canWrite, canExecute, isDirectory)
  }
  val directoryTable = TableQuery[DirectoryTable]

  class ChecksumTable(tag: Tag)
    extends Table[(String, String)](tag, "CHECKSUM") {
    def name = column[String]("NAME", O.PrimaryKey)
    def md5 = column[String]("MD5")
    def * = (name, md5)
  }
  val checksumTable = TableQuery[ChecksumTable]

  val dbroot = envOrElse("SFSROOT", userDir)
  val dbpath = new File(dbroot + "/sfsdb").getAbsolutePath
  val url = "jdbc:h2:" + dbpath
  val driver = "org.h2.Driver"

  def create =
    Database.forURL(url, driver = driver) withSession { implicit session =>
      directoryTable.ddl.create
      checksumTable.ddl.create
    }

  def listAllFiles(file: File): Array[File] = {
    val list = file.listFiles
    if (list == null)
      Array[File]()
    else
      list ++ list.filter(_.isDirectory).flatMap(listAllFiles)
  }

  def checkFile(file: File) = {
    try {
      val fIS = new BufferedInputStream(new FileInputStream(file))
      val md5 = md5Hex(fIS)
      fIS.close
      md5
    } catch {
      case e: Exception => ""
    }
  }

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def gather(fileName: String) = {
    val file = new File(fileName)
    val files =
      if (file.exists)
        if (file.isDirectory) listAllFiles(file) else Array(file)
      else
        Array[File]()

    val total = files.length
    if (total > 0) {
      val delta = if (total < 100) 1 else total / 100
      val filesWithIndex = files.zipWithIndex

      Database.forURL(url, driver = driver) withSession { implicit session =>
        for ((f, i) <- filesWithIndex) {
          directoryTable += (
            f.getAbsolutePath,
            f.length,
            Date.valueOf(dateFormat.format(f.lastModified)),
            f.canRead,
            f.canWrite,
            f.canExecute,
            f.isDirectory)

          if (f.isFile)
            checksumTable += (
              f.getAbsolutePath,
              checkFile(f))

          if (i % delta == 0) print("importing [%2d%%]\r".format(i * 100 / total))
        }
        print("importing [100%]")
      }
    }
  }

  def runQuery(sqlFile: String) = {
    val sql = Source.fromFile(new File(sqlFile)).mkString
    Database.forURL(url, driver = driver) withSession { implicit session =>
      val statement = session.conn.createStatement
      try {
        val rs = statement.executeQuery(sql)
        val rsmd = rs.getMetaData
        val cols = rsmd.getColumnCount
        while (rs.next()) {
          val row = (1 to cols) map { rs.getString } mkString ("; ")
          println(row)
        }
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
  }

  def runUpdate(sqlFile: String) = {
    val sql = Source.fromFile(new File(sqlFile)).mkString
    Database.forURL(url, driver = driver) withSession { implicit session =>
      val statement = session.conn.createStatement
      try {
        val result = statement.executeUpdate(sql)
        println("Return: " + result)
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
  }
}