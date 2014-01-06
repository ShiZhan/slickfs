object slickfs extends App {
  println("usage: slickfs <SQL>")
  args.lift(0).getOrElse(null) match {
    case input: String => database.Directory.createTable(new java.io.File(input))
    case _ => println("incorrent parameter")
  }
}