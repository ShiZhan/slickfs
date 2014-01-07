package database

object SQLHandler {
  import slick.driver.H2Driver.backend.Session

  def query(sql: String)(implicit session: Session) = {
    val statement = session.conn.createStatement
    try { statement.executeQuery(sql) }
    catch { case e: Exception => e }
  }

  def update(sql: String)(implicit session: Session) = {
    val statement = session.conn.createStatement
    try { statement.executeUpdate(sql) }
    catch { case e: Exception => e }
  }
}