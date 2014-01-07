package database

object FSDatabase {
  import java.io.File
  import java.sql.{ Date, ResultSet }
  import java.text.SimpleDateFormat
  import scala.io.Source
  import scala.slick.driver.H2Driver.simple._
  import scala.util.Properties.{ envOrElse, userDir }

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

  val db_file = new File(envOrElse("SFS_DB", userDir)).getAbsolutePath
  val url = "jdbc:h2:" + db_file
  val driver = "org.h2.Driver"
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def create =
    Database.forURL(url, driver = driver)
      .withSession { implicit session => directoryTable.ddl.create }

  def list =
    Database.forURL(url, driver = driver) withSession { implicit session =>
      directoryTable foreach {
        case (path, size, date, r, w, x, d) =>
          println(path + "\t" + size + "\t" + date + "\t" + r + ":" + w + ":" + x + ":" + d)
      }
    }

  def listAllFiles(f: File): Array[File] = {
    val list = f.listFiles
    if (list == null)
      Array[File]()
    else
      list ++ list.filter(_.isDirectory).flatMap(listAllFiles)
  }

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

          if (i % delta == 0) print("importing [%2d%%]\r".format(i * 100 / total))
        }
        print("importing [100%]")
      }
    }
  }

  def runQuery(sqlFile: String) = {
    val sql = Source.fromFile(new File(sqlFile)).mkString
    Database.forURL(url, driver = driver) withSession { implicit session =>
      SQLHandler.query(sql) match {
        case rs: ResultSet => {
          val rsmd = rs.getMetaData
          val cols = rsmd.getColumnCount
          while (rs.next()) {
            val row = (1 to cols) map { rs.getString } mkString ("; ")
            println(row)
          }
        }
        case e: Exception => e.printStackTrace
      }
    }
  }

  def runUpdate(sqlFile: String) = {
    val sql = Source.fromFile(new File(sqlFile)).mkString
    Database.forURL(url, driver = driver) withSession { implicit session =>
      SQLHandler.update(sql) match {
        case result: Int => println("Return: " + result)
        case e: Exception => e.printStackTrace
      }
    }
  }
}