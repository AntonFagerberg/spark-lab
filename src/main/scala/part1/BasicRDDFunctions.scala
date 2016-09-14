package part1

import org.apache.spark.rdd.RDD

/** Part 1: Basic RDD functions
  *
  * In this part, we will apply functions to an RDD containing Pride and Prejudice by Jane Austen.
  *
  * All the functions you need to implement these methods are found here:
  * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD
  *
  * If you get stuck, you can look at the tests for clues.
  */
object BasicRDDFunctions {

  /** The book needs more screaming!
    * MAKE ALL THE LINES UPPER CASE!
    */
  def makeItUpperCase(prideAndPrejudice: RDD[String]): RDD[String] = {
    prideAndPrejudice.map(_.toUpperCase)
  }

  /** Remove all lines which doesn't contain "Darcy" (case sensitive).
    */
  def mrDarcy(prideAndPrejudice: RDD[String]): RDD[String] = {
    prideAndPrejudice.filter(_.contains("Darcy"))
  }

  /** How many words are there?
    *
    * Words are separated by a space character ' '.
    * To make it simple, we assume that "Hello", "hello" and "Hello!" are different words.
    */
  def howManyWords(prideAndPrejudice: RDD[String]): Long = {
    prideAndPrejudice
      .flatMap(_.split(' '))
      .count()
  }


  /** How many characters is there in the shortest non-empty line?
    *
    * (Characters as in the "chars" that make up a string, not Mr. Darcy...)
    */
  def shortestNonEmptyLine(prideAndPrejudice: RDD[String]): Int = {
    prideAndPrejudice
      .map(_.length)
      .filter(_ > 0)
      .min
  }


  /** Return a string with every 'a' in the text.
    */
  def aIsMyFavourite(prideAndPrejudice: RDD[String]): String = {
    prideAndPrejudice
      .map(_.filter(_ == 'a'))
      .reduce(_ + _)
  }
}
