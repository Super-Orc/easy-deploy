name := "easy deploy tool for hadoop ecosystem"

version := "1.0"

resolvers += "JAnalyse Repository" at "http://www.janalyse.fr/repository/"

libraryDependencies ++= Seq(
  "fr.janalyse" %% "janalyse-ssh" % "0.9.14" % "compile"
)
