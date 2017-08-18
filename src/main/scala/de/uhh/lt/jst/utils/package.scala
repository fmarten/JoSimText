package de.uhh.lt.jst

package object utils {
  /**
    * Adds getOrElse to Array[String] for convenient param accessing with defaults
    */
  implicit class ArgsGetOrElseSupport(args: Array[String]) extends SomeArgs(args)
}
