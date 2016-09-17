package part1

import org.apache.spark.rdd.RDD

/** Part 1: Basic RDD functions
  *
  * In this part, we will apply functions to an RDD containing Pride and Prejudice by Jane Austen.
  * The RDD items are lines from the text (a line as in the number of words that fit based on the
  * width of the page, not a full sentence).
  *
  * All the functions you need to implement these methods are found here:
  * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD
  *
  * If you get stuck in part 1, you can look at the tests for clues.
  */
object Part1 {

  /** The book needs more screaming!
    * MAKE ALL THE LINES UPPER CASE!
    */
  def makeItUpperCase(prideAndPrejudice: RDD[String]): RDD[String] = {
    ???
  }

  /** Remove all lines which doesn't contain "Darcy" (case sensitive).
    */
  def mrDarcy(prideAndPrejudice: RDD[String]): RDD[String] = {
    ???
  }

  /** Return all the words.
    *
    * Words are separated by a space character ' '.
    * (To make it simple, we assume that "Hello", "hello" and "Hello!" are different words.)
    */
  def allTheWords(prideAndPrejudice: RDD[String]): RDD[String] = {
    ???
  }

  /** How many words are there?
    *
    * Words are separated by a space character ' '.
    * (To make it simple, we assume that "Hello", "hello" and "Hello!" are different words.)
    */
  def howManyWords(prideAndPrejudice: RDD[String]): Long = {
    ???
  }


  /** How many characters is there in the shortest non-empty line?
    *
    * (Characters as in the "chars" that make up a string, not Mr. Darcy...)
    */
  def shortestNonEmptyLine(prideAndPrejudice: RDD[String]): Int = {
    ???
  }


  /** Return a string with every 'a' in the text.
    */
  def aIsMyFavourite(prideAndPrejudice: RDD[String]): String = {
    ???
  }
}
