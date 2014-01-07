package database

object SQLHandler {
  import slick.driver.H2Driver.backend.Session

  def query(sql: String)(implicit session: Session) = {
    val statement = session.conn.createStatement
    try {
      val rs = statement.executeQuery(sql)
      val rsmd = rs.getMetaData
      val cols = rsmd.getColumnCount
      while (rs.next()) {
        val row = (1 to cols) map { rs.getString(_) } mkString ("; ")
        println(row)
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
  }

  def update(sql: String)(implicit session: Session) = {
    val statement = session.conn.createStatement
    try {
      val result = statement.executeUpdate(sql)
      println("Return: " + result)
    } catch {
      case e: Exception => e.printStackTrace
    }
  }
}