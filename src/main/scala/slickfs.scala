object slickfs extends App {
  import database.FSDatabase.{ create, gather, runQuery, runUpdate }

  val usage = "slickfs {-c|-i <File|Directory>|-q <SQL>|-u <SQL>}"
  args.toList match {
    case Nil => println(usage)
    case "-c" :: Nil => create
    case "-i" :: inputFN :: Nil => gather(inputFN)
    case "-q" :: queryFN :: Nil => runQuery(queryFN)
    case "-u" :: updateFN :: Nil => runUpdate(updateFN)
    case _ => println("incorrent parameter")
  }
}