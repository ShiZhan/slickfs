object slickfs extends App {
  import database.FSDatabase

  val usage = "slickfs {-c|-i <File|Directory>|-q <SQL>|-u <SQL>}"
  args.toList match {
    case Nil => println(usage)
    case "-c" :: Nil => FSDatabase.initialize
    case "-i" :: input :: Nil => FSDatabase.gather(new java.io.File(input))
    case "-q" :: query :: Nil => FSDatabase.query(query)
    case "-u" :: query :: Nil => FSDatabase.update(query)
    case _ => println("incorrent parameter")
  }
}