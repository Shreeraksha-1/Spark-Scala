object Superheroes {

  def main(args: Array[String]): Unit = {
    // Given data
    val realNames = List("peter parker", "clarke kent", "robert", "bruce")
    val heroName = List("spiderman", "superman", "hulk", "batman")

    val heroMovies = Map[String, List[String]](
      "spiderman" -> List("spiderman 1", "amazing spiderman", "far from home", "avengers"),
      "superman" -> List("quest for peace", "man of steel", "justice league"),
      "hulk" -> List("incredible hulk", "incredible hulk"),
      "batman" -> List("dark night", "justice league")
    )

    // Create a list of maps (superhero name -> real name)
    val superheroToRealName = heroName.zip(realNames).map { case (hero, real) => hero -> real }

    // Print the list of maps and their movies
    superheroToRealName.foreach { case (superhero, realname) =>
      println(s"Superhero: $superhero, Real Name: $realname")
      println(s"Movies: ${heroMovies.getOrElse(superhero, List())}")
      println()
    }
  }

}
