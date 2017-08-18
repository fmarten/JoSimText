package de.uhh.lt.jst.utils

case class SomeArgs(args: Array[String]) {
  private val internal: Map[Int, String] = args.toList.zipWithIndex.map(_.swap).toMap

  def getOrElse(key: Int, default: String): String = internal.getOrElse(key, default)

  def getAsIntOrElse(key: Int, default: Int): Int = internal.get(key) match {
    case Some(value) => value.toInt
    case None => default
  }

  def getAsDoubleOrElse(key: Int, default: Double): Double = internal.get(key) match {
    case Some(value) => value.toDouble
    case None => default
  }
}

