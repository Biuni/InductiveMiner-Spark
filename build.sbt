// **********************************
//  Inductive Miner on Apache Spark
// ----------------------------------
// Gianluca Bonifazi - Gianpio Sozzo
// **********************************

// ============================================================================

scalaVersion := "2.11.12"

// ============================================================================

name := "inductive-miner"
organization := "ch.epfl.scala"
version := "1.0"

lazy val spark = "org.apache.spark"
lazy val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  spark %% "spark-core" % sparkVersion,
  spark %% "spark-graphx" % sparkVersion
)

// ============================================================================

