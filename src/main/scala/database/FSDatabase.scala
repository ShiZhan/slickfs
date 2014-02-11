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
  import common.FileEx.FileOps
  import common.Gauge._

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
    if (files.length > 0)
      Database.forURL(url, driver = driver) withSession { implicit session =>
        files.forAllDo { f =>
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
        }
      }
  }

  def runQuery(sqlFile: String) = {
    val sql = Source.fromFile(new File(sqlFile)).mkString
    Database.forURL(url, driver = driver) withSession { implicit session =>
      try {
        val (r, t) = timedOp { () => Q.queryNA[FileEntry](sql).foreach(println) }
        println("Query executed in %d milliseconds".format(t))
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
  }

  def runUpdate(sqlFile: String) = {
    val sql = Source.fromFile(new File(sqlFile)).mkString
    Database.forURL(url, driver = driver) withSession { implicit session =>
      try {
        val (r, t) = timedOp { () => (Q.u + sql).execute }
        println("Query executed in %d milliseconds".format(t))
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
  }
}