package part2

import org.apache.spark.rdd.RDD
import setup.User

/** Part 2: Pair RDD functions
  *
  * In this part, we will apply functions using pair RDDs created from user data.
  *
  * In addition to the functions we used in part1:
  * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD
  *
  * We will also need to use these functions:
  * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions
  *
  * (In this part, it's harder to look at the tests for clues).
  */
object Part2 {

  /** Create a pair RDD with the age used as the key.
    */
  def keyedByAge(users: RDD[User]): RDD[(Int, User)] = {
    ???
  }

  /** Count the number of unique ages.
    *
    * Return an pair RDD with the age as key and the number of users with
    * that age as the value.
    */
  def ageCount(users: RDD[User]): RDD[(Int, Int)] = {
    ???
  }

  /** What is the most common age - and how many has that age?
    *
    * Return a tuple with (age, count)
    */
  def mostCommonAge(users: RDD[User]): (Int, Int) = {
    ???
  }

  /** Return a pair RDD:
    * The key should be the first letter of the (first) name.
    * The value should be a list of all the users with a (first) name
    * that starts with that letter..
    *
    * Example: ('A', List(User("Adam", ...), User("Albert", ...), ...)
    */
  def usersGroupedByFirstLetter(users: RDD[User]): RDD[(Char, List[User])] = {
    ???
  }

  /** Some names have nicknames.
    * You will receive two RDDs. One contains users as before.
    * The other contains a tuple with (name, nickname).
    *
    * Return RDD with (nickname, surname) for all users that has a nickname.
    */
  def nickNames(users: RDD[User], nicknames: RDD[(String, String)]): RDD[(String, String)] = {
    ???
  }

  /** Like before, but if a user has no nickname, use "Nope" as the nickname instead.
    *
    * Return RDD with (nickname, surname) for all users.
    */
  def nickNamesWithDefaultValue(users: RDD[User], nicknames: RDD[(String, String)]): RDD[(String, String)] = {
    ???
  }

  /** CombineByKey is a very powerful function similar to reduce, but with a different return type.
    * To use it, we will have to implement three methods: createCombiner, mergeValue and mergeCombiners.
    *
    * For every unique age, return the name(s) with the shortest length.
    *
    * Return RDD with (age, list of names).
    * Example: RDD((56,List(Joel, Leah, Rhea, Roth, Sade, Yoko)), (40,List(Ima)), (41,List(Colt, Todd)), ...)
    */
  def shortestNamesByAge(users: RDD[User]): RDD[(Int, List[String])] = {

    /** The createCombiner turns our value (User) into the format we want
      * to return, i.e. a list of (first) names.
      */
    def createCombiner(user: User): List[String] = {
      ???
    }

    /** mergeValue is called in when we have to merge a value (User) with our
      * return value (list of names).
      *
      * We have to handle three outcomes:
      * The User has a shorter name than the list of names.
      * The list of names are shorter than the User's name.
      * The name in User is the same length as the list of names.
      */
    def mergeValue(names: List[String], user: User): List[String] = {
      ???
    }

    /** Merge combiners is used to merge two return values (list of names).
      *
      * We have to handle three outcomes:
      * The names in names1 has the shortest lengths.
      * The names in names2 has the shortest lengths.
      * The names in names1 and names2 have equal lengths.
      */
    def mergeCombiners(names1: List[String], names2: List[String]): List[String] = {
      ???
    }

    users
      .keyBy(_.age) // Key all our users by age.
      .combineByKey(createCombiner, mergeValue, mergeCombiners)
  }
}
