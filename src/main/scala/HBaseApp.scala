// Library files.
import org.apache.spark.{
  SparkConf,
  SparkContext}

/**
  * Created by peterbugaj on 2016-07-15.
  */
object SessionizeApp {

  private val _sparkContext = this._createSparkContext

  /**
    * Main function.
    */
  def main(args: Array[String]) {

    // Run test cases first before analyzig the actual data.
    runTests()

    // If all test passed, execute the sessionizer on the actual data.
    val input = this._sparkContext.textFile("data/realSample.log")
    Sessionizer.computeAverageSessionTime(input).
      saveAsTextFile("sessionsPerUser")
    Sessionizer.computeUniqueVisits(input).
      saveAsTextFile("uniqueVisits")
  }

  /**
    * Method for running test cases.
    */
  def runTests(): Unit = {

    this.test_1_uniqueVisitPerUser()

    this.test_2_sessionsPerOneUser()

    this.test_3_sessionsPerMultipleUsers()

    this.test_4_sessionsWithZeroLength()

    this.test_5_sessionsWithLength()

    this.test_6_sessionsAverages()

    this.test_7_sessionsAveragesWithZeroLength()

    this.test_8_longestSessionOneUser()

    this.test_9_longestSessionMultipleUsers()

    this.test_10_getMostEngagedUsers()

    this.test_11_useUnsortedData()
  }

  /**
    * Check that the unique URL visits per user are counted correctly.
    */
  def test_1_uniqueVisitPerUser(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/1_uniqueVisits")

    val results = Sessionizer.computeUniqueVisits(input).collect
    assert(results.length == 3)

    val uniqueVisitsPerUser = Array(
      ("123.242.248.130:54635", 2),
        ("1.39.32.179:56419", 3),
        ("59.183.41.47:62014", 2))

    results.foreach(p => {
      assert(uniqueVisitsPerUser.contains(p))
    })
  }

  /**
    * Check that the session are counted correctly for one user.
    */
  def test_2_sessionsPerOneUser(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/2_userSessions")

    val results = Sessionizer.computeAverageSessionTime(input)
    assert(results.count == 1)

    val uniqueVisitsPerUser = Array(
      ("123.242.248.130:54635", 4))

    results.foreach(p => {
      assert(uniqueVisitsPerUser.contains((p._1, p._2.count)))
    })
  }

  /**
    * Check that the session are counted correctly for multiple users.
    */
  def test_3_sessionsPerMultipleUsers(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/3_multipleUserSessions")

    val results = Sessionizer.computeAverageSessionTime(input).collect

    assert(results.length == 3)

    val uniqueVisitsPerUser = Array(
      ("123.242.248.130:54635", 4),
      ("599.242.248.130", 2),
      ("400.242.248.130", 1))

    results.foreach(p => {
      assert(uniqueVisitsPerUser.exists(a => a._2 == p._2.count))
    })
  }

  /**
    * Check that the session are of correct length in the case where
    * the user visited the site only once during a session. In other
    * words the sessions are of length zero.
  */
  def test_4_sessionsWithZeroLength(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/4_instantSessions")

    val results = Sessionizer.computeAverageSessionTime(input).collect

    assert(results.length == 3)

    results.foreach(p => {
      assert(p._2.totalLength == 0)
    })
  }

  /**
    * Check that the session are of correct length for each user IP
    */
  def test_5_sessionsWithLength(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/5_sessionLengths")

    val results = Sessionizer.computeAverageSessionTime(input).collect

    assert(results.length == 3)

    val lengthsPerUser = Array(
      ("123.242.248.130:54635", 240000),
      ("599.242.248.130", 1000),
      ("400.242.248.130", 300000))

    results.foreach(p => {
      assert(lengthsPerUser.exists(a => a._2 == p._2.totalLength))
    })
  }

  /**
    * Check that the session averages are correctly computed per user IP.
    */
  def test_6_sessionsAverages(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/6_sessionAverages")

    val results = Sessionizer.computeAverageSessionTime(input).collect

    assert(results.length == 3)

    val lengthsPerUser = Array(
      ("123.242.248.130:54635", 80000),
      ("599.242.248.130", 1000),
      ("400.242.248.130", 150000))

    results.foreach(p => {
      assert(lengthsPerUser.exists(a => a._2 == p._3))
    })
  }

  /**
    * Check that the session averages are correctly computed,
    * where some of the sessions are of length zero.
    */
  def test_7_sessionsAveragesWithZeroLength(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/7_sessionAveragesWithZeroLength")

    val results = Sessionizer.computeAverageSessionTime(input).collect

    assert(results.length == 6)

    val lengthsPerUser = Array(
      ("123.242.248.130:54635", 80000),
      ("599.242.248.130", 1000),
      ("400.242.248.130", 150000),
      ("900.242.248.130", 0),
      ("100.242.248.130", 0),
      ("777.242.248.130", 0))

    results.foreach(p => {
      assert(lengthsPerUser.exists(a => a._2 == p._3))
    })
  }

  /**
    * Find the longest session for one specific user IP address
    */
  def test_8_longestSessionOneUser(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/8_longestSessionsUser")

    val results = Sessionizer.computeAverageSessionTime(input).collect

    assert(results.length == 1)

    assert(results.head._2.longest == 1320000)
  }

  /**
    * Find the longest session for multiple different users IP
    * addresses, with each user having multiple sessions
    */
  def test_9_longestSessionMultipleUsers(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/9_longestSessionsMultipleUsers")

    val results = Sessionizer.computeAverageSessionTime(input).collect

    assert(results.length == 6)

    val lengthsPerUser = Array(
      ("123.242.248.130:54635", 180000),
      ("599.242.248.130", 1000),
      ("400.242.248.130", 300000),
      ("900.242.248.130", 0),
      ("100.242.248.130", 0),
      ("777.242.248.130", 1320000))

    results.foreach(p => {
      assert(lengthsPerUser.exists(a => a._2 == p._2.longest))
    })
  }

  /**
    * Ensure that the data is returned, sorted by users with the longest
    * session length, so to be able to see the most engaged users on top.
    */
  def test_10_getMostEngagedUsers(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/10_getMostEngagedUsers")

    val results = Sessionizer.computeAverageSessionTime(input).collect

    assert(results.length == 6)

    // Check that the top three users are in order.
    assert(results(0)._2.longest == 1320000)
    assert(results(1)._2.longest == 300000)
    assert(results(2)._2.longest == 180000)
  }

  /**
    * Ensure that everything still works and that the longest session lengths
    * are returned, even when the input data is not sorted by request times.
    */
  def test_11_useUnsortedData(): Unit = {

    // Check that the unique visits per user are counted correctly.
    val input = this._sparkContext.textFile("tests/11_unsortedData")

    val results = Sessionizer.computeAverageSessionTime(input).collect

    assert(results.length == 6)

    val lengthsPerUser = Array(
      ("123.242.248.130:54635", 180000),
      ("599.242.248.130", 1000),
      ("400.242.248.130", 300000),
      ("900.242.248.130", 0),
      ("100.242.248.130", 0),
      ("777.242.248.130", 1320000))

    results.foreach(p => {
      assert(lengthsPerUser.exists(a => a._2 == p._2.longest))
    })
  }

  /**
    * Creates a spark context.
    */
  private def _createSparkContext: SparkContext = {
    val conf = new SparkConf().setAppName("Basic Spark Application")
    new SparkContext(conf)
  }
}
