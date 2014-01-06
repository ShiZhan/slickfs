object slickfs extends App {
  import database.FSDatabase

  FSDatabase.initialize
  args.toList match {
    case Nil => println("usage: slickfs {-i <File|Directory>|-q <SQL>}")
    case "-i" :: input :: Nil => FSDatabase.gather(new java.io.File(input))
    case "-q" :: query :: Nil => FSDatabase.query(query)
    case "-u" :: query :: Nil => FSDatabase.update(query)
    case _ => println("incorrent parameter")
  }
}