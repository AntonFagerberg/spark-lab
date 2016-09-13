# Spark lab
Clone this repository!

## Installation
* Install [Java JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html).

In this lab we will use Gradle, Spark and Scala but you do not have to install any of them yourself - it should all be taken care of by the included Gradle wrapper!

### Setup IntelliJ IDEA (recommended)
Install [IntelliJ IDEA Community](https://www.jetbrains.com/idea/#chooseYourEdition).

#### Open project in IntelliJ IDEA
If you haven't used IntelliJ IDEA before, be sure to install the Scala plugin when you reach "Download featured plugins" during the setup wizard. Default settings are ok otherwise.

 * Start IntelliJ IDEA.
 * If this is your first JVM project:
  * Click `Configure > Project Defaults > Project Structure`.
  * Under `Project` and `Project SDK`, click `New...` and select `JDK`.
    * Windows: `C:\Program Files\Java\jdk1.8.0_<version>`
    * OS X: `/Library/Java/JavaVirtualMachines/jdk1.8.0_<version>.jdk/Contents/H‌​ome`
    * Linux: `/usr/lib/jvm(/java-8-oracle)` (can be different depending on your distribution)
 * Click "Open".
 * Locate project folder `spark-lab` and select it.
 * Click "OK".
 * Use the default settings but make sure Gradle JVM is 1.8.
 * Click "OK".
 * Wait for everything to download and build - it will take a little while.

If you haven't already been asked to install the Scala plugin yet, do it now (`Preferences > Plugins > Install JetBrains plugin...`)

## Running tests

The code for `part0` is already implemented, you should make sure the test runs as expected (see below).

### IntelliJ IDEA
IntelliJ IDEA need to download the dependencies and index the project. In the bottom right corner, if you have a text displaying `<nr> process(es) running...`, wait for it to finish (otherwise the `Run` menu will not be visible).

In the left side-bar (`Cmd + 1` or `Alt + 1` if not visible), expand `spark-lab > src > test > scala`.

Right click on the `part0` package, click `Run > ScalaTests in 'part0...` (the bottom one with the little [Scala icon](http://www.scala-lang.org/resources/img/smooth-spiral.png)). 

It's recommended to use `ScalaTests ...` (the bottom one with the little Scala icon) and not `Test ...`, otherwise things can get a little screwed up if you wish to run individual tests later.

#### Optional

You can also open any test source file and right click either on the class name (`AlreadyImplementedTest` in `part0` to run all the tests in that class), or on the line `test("...` (to run a specific test). As before, click `Run > ScalaTests in 'part0...` (the bottom one with the little Scala icon). 

### Command line
`./gradlew test --tests part0*` (OS X / Linux)

`gradlew.bat test --tests part0*` (Windows)

#### Optional
Run all tests:

`./gradlew test` (OS X / Linux)

`gradlew.bat test` (Windows)

Advanced options: http://stackoverflow.com/a/31468902

## Doing the lab
The lab is divided into several parts, you should preferably do them in order although it is not required.

To get started, navigate to the first package `src > main > scala > part1` and double-click the source file.

You'll find further instructions about what to do in each source file.

Implement all the methods by replacing the `???` with your (hopefully) working code.

Verify that your solutions are correct by running the corresponding unit tests, `src > test > scala > part1`.

### Spark UI (optional)
While it's not needed, it can be interesting to look at the Spark UI while the tests are running. Unfortunately, the test will probably finish before you'll have a chance to look at it.

To overcome this, you can go to `src > test > scala > setup` and open the `SparkTest` trait. In the `afterAll()` method, before `sparkContext.stop()`, add the following line `Thread.sleep(Long.MaxValue)`.

While running a specific test, look for the following line in the output:
`16/09/12 19:49:48 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.255.147:4040`

Click the link to see the Spark UI.

Remember to remove the `Thread.sleep` when you're done, otherwise your tests will continue to hang indefinitely.
