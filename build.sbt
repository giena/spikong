name := "spikong"

version := "0.0.1"

organization := "awl.spikong"

scalaVersion := "2.12.1"

//fork in Test := true

fork in run := true


licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

libraryDependencies ++= Seq(
	"com.typesafe.akka" %% "akka-actor" % "2.4.16",
	"com.typesafe.akka" %% "akka-cluster" % "2.4.16",
	"com.typesafe.akka" %% "akka-cluster-tools" % "2.4.16",
	"com.typesafe.akka" %% "akka-cluster-sharding" % "2.4.16",
	"com.typesafe.akka" %% "akka-contrib" % "2.4.16",
	"com.typesafe.akka" %% "akka-multi-node-testkit" % "2.4.16" % "test",
	"com.typesafe.akka" %% "akka-slf4j" % "2.4.16",
  "com.tapad.scaerospike" %% "scaerospike" % "1.2.6",
  "junit" % "junit" % "4.12" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "com.codahale.metrics" % "metrics-core" % "3.0.2",
	"ch.qos.logback" % "logback-classic" % "1.2.1"
 )
 

 resolvers ++= Seq(
 	"Maven Central" at "http://central.maven.org/maven2",
 	"Akka Repository" at "http://repo.akka.io/releases/",
		"Thrift" at "http://people.apache.org/~rawson/repo/",
			"Apache HBase" at "https://repository.apache.org/content/repositories/releases",
				"Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
					"krasserm at bintray" at "http://dl.bintray.com/krasserm/maven", 
						"kazan snapshots" at "http://kazan.priv.atos.fr/nexus/content/repositories/snapshots/",
						"Bintray Worldline" at "http://dl.bintray.com/worldline-messaging-org/maven/")

publishTo := Some("Bintray API Realm" at "https://api.bintray.com/maven/worldline-messaging-org/maven/spikong")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
