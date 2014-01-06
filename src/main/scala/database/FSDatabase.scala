package database

object FSDatabase {
  import java.io.File
  import java.sql.Date
  import java.text.SimpleDateFormat
  import scala.slick.driver.H2Driver.simple._
  import scala.util.Properties.{ envOrElse, userDir }

  class DirectoryTable(tag: Tag)
    extends Table[(String, Long, Date, Boolean, Boolean, Boolean, Boolean)](tag, "DIRECTORY") {
    def name = column[String]("Name", O.PrimaryKey)
    def fileSize = column[Long]("FileSize")
    def lastMod = column[Date]("LastModified")
    def canRead = column[Boolean]("canRead")
    def canWrite = column[Boolean]("canWrite")
    def canExecute = column[Boolean]("canExecute")
    def isDirectory = column[Boolean]("isDirectory")
    def * = (name, fileSize, lastMod, canRead, canWrite, canExecute, isDirectory)
  }
  val directoryTable = TableQuery[DirectoryTable]

  val db_file = new File(envOrElse("SFS_DB", userDir)).getAbsolutePath
  val url = "jdbc:h2:" + db_file
  val driver = "org.h2.Driver"
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def initialize =
    try {
      Database.forURL(url, driver = driver)
        .withSession { implicit session => directoryTable.ddl.create }
    } catch {
      case e: Exception => println(e)
    }

  def listAllFiles(f: File): Array[File] = {
    val list = f.listFiles
    if (list == null)
      Array[File]()
    else
      list ++ list.filter(_.isDirectory).flatMap(listAllFiles)
  }

  def flattenFiles(file: File) =
    if (file.exists)
      if (file.isDirectory) listAllFiles(file) else Array(file)
    else
      Array[File]()

  def gather(file: File) = {
    val files = flattenFiles(file)
    Database.forURL(url, driver = driver) withSession { implicit session =>
      for (f <- files)
        directoryTable += (
          f.getAbsolutePath,
          f.length,
          Date.valueOf(dateFormat.format(f.lastModified)),
          f.canRead,
          f.canWrite,
          f.canExecute,
          f.isDirectory)
    }
  }

  def query(sql: String) = {
    Database.forURL(url, driver = driver) withSession { implicit session =>
      directoryTable foreach {
        case (path, size, date, r, w, x, d) =>
          println(path + "\t" + size + "\t" + date + "\t" + r + ":" + w + ":" + x + ":" + d)
      }
    }
  }
}