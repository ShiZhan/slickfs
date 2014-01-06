package database

object Directory {
  import java.io.File
  import java.sql.Date
  import java.text.SimpleDateFormat
  import scala.slick.driver.H2Driver.simple._

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

  val url = "jdbc:h2:mem:directory"
  val driver = "org.h2.Driver"
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def createTable(file: File) = {
    val files = flattenFiles(file)
    Database.forURL(url, driver = driver) withSession {
      implicit session =>
        directoryTable.ddl.create
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
}