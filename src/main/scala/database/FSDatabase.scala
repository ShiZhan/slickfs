package database

object FSDatabase {
  import java.io.File
  import java.sql.Date
  import scala.io.Source
  import scala.slick.driver.H2Driver.simple._
  import scala.slick.jdbc.{ GetResult, StaticQuery => Q }
  import Q.interpolation
  import scala.util.Properties.{ envOrElse, userDir }
  import scala.compat.Platform.currentTime
  import helper.FileEx.FileOps

  class DirectoryTable(tag: Tag)
    extends Table[(String, Long, Date, Boolean, Boolean, Boolean, Boolean, String)](tag, "DIRECTORY") {
    def name = column[String]("NAME", O.PrimaryKey)
    def fileSize = column[Long]("FILESIZE")
    def lastMod = column[Date]("LASTMODIFIED")
    def canRead = column[Boolean]("R")
    def canWrite = column[Boolean]("W")
    def canExecute = column[Boolean]("X")
    def isDirectory = column[Boolean]("D")
    def upperLevel = column[String]("UP")
    def * = (name, fileSize, lastMod, canRead, canWrite, canExecute, isDirectory, upperLevel)
  }
  val directoryTable = TableQuery[DirectoryTable]

  class ChecksumTable(tag: Tag) extends Table[(String, String)](tag, "CHECKSUM") {
    def name = column[String]("NAME", O.PrimaryKey)
    def md5 = column[String]("MD5")
    def * = (name, md5)
  }
  val checksumTable = TableQuery[ChecksumTable]

  case class FileEntry(name: String, size: Long, lastMod: Date, canRead: Boolean,
    canWrite: Boolean, canExecute: Boolean, isDirectory: Boolean, upperLevel: String) {
    override def toString =
      name + ';' + size + ';' + lastMod + ';' + canRead + ';' + canWrite + ';' +
        canExecute + ';' + isDirectory + ';' + upperLevel
  }
  implicit val getFileEntryResult =
    GetResult(r => FileEntry(r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<, r.<<))

  val dbroot = envOrElse("SFSROOT", userDir)
  val dbpath = new File(dbroot + "/sfsdb").getAbsolutePath
  val url = "jdbc:h2:" + dbpath
  val driver = "org.h2.Driver"

  def create =
    Database.forURL(url, driver = driver) withSession { implicit session =>
      directoryTable.ddl.create
      checksumTable.ddl.create
    }

  def gather(fileName: String) = {
    val files = new File(fileName).flatten
    val total = files.length
    if (total > 0) {
      val delta = if (total < 100) 1 else total / 100
      val filesWithIndex = files.zipWithIndex

      Database.forURL(url, driver = driver) withSession { implicit session =>
        for ((f, i) <- filesWithIndex) {
          directoryTable += (
            f.getAbsolutePath,
            f.length,
            f.lastModifiedString,
            f.canRead,
            f.canWrite,
            f.canExecute,
            f.isDirectory,
            f.getParentFile match { case p: File => p.getAbsolutePath; case _ => "N/A" })
          if (f.isFile)
            checksumTable += (
              f.getAbsolutePath,
              f.checksum)

          if (i % delta == 0) print("importing [%2d%%]\r".format(i * 100 / total))
        }
        print("importing [100%]")
      }
    }
  }

  def runQuery(sqlFile: String) = {
    val sql = Source.fromFile(new File(sqlFile)).mkString
    Database.forURL(url, driver = driver) withSession { implicit session =>
      try {
        val t1 = currentTime
        val rs = Q.queryNA[FileEntry](sql)
        val t2 = currentTime

        rs.foreach(println)
        println("Query executed in %d milliseconds".format(t2 - t1))
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
  }

  def runUpdate(sqlFile: String) = {
    val sql = Source.fromFile(new File(sqlFile)).mkString
    Database.forURL(url, driver = driver) withSession { implicit session =>
      try {
        val t1 = currentTime
        (Q.u + sql).execute
        val t2 = currentTime

        println("Query executed in %d milliseconds".format(t2 - t1))
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
  }
}