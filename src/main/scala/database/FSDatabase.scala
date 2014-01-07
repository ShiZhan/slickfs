package database

object FSDatabase {
  import java.io.File
  import java.sql.{ Date, DriverManager }
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

  def list = {
    Database.forURL(url, driver = driver) withSession { implicit session =>
      directoryTable foreach {
        case (path, size, date, r, w, x, d) =>
          println(path + "\t" + size + "\t" + date + "\t" + r + ":" + w + ":" + x + ":" + d)
      }
    }
  }

  def listAllFiles(f: File): Array[File] = {
    val list = f.listFiles
    if (list == null)
      Array[File]()
    else
      list ++ list.filter(_.isDirectory).flatMap(listAllFiles)
  }

  def gather(file: File) = {
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

  def query(sql: String) = {
    val connection = DriverManager.getConnection(url, "", "")
    try {
      val statement = connection.createStatement
      val rs = statement.executeQuery(sql)
      val rsmd = rs.getMetaData
      val cols = rsmd.getColumnCount
      while (rs.next()) {
        val row = (1 to cols) map { rs.getString(_) } mkString ("; ")
        println(row)
      }
    } catch {
      case e: Exception => e.printStackTrace
    } finally {
      connection.close()
    }
  }

  def update(sql: String) = {
    val connection = DriverManager.getConnection(url, "", "")
    try {
      val statement = connection.createStatement
      val result = statement.executeUpdate(sql)
      println("Return: " + result)
    } catch {
      case e: Exception => e.printStackTrace
    } finally {
      connection.close()
    }
  }
}