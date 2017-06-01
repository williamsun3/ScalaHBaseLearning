// Library files.
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD

/**
  * Created by peterbugaj on 2016-07-17.
  */
object Sessionizer {

  /**
    * The time frame window to use for detecting new session (15 minutes).
    */
  private val _inactiveWindowMillis: Long = 900000l

  /**
    * Determine the session information per user.
    */
  def computeAverageSessionTime(input: RDD[String]): RDD[(String, SessionInfo, Double)] = {

    // Parse for the necessary information first
    // and sort by the time stamp.
    val requestsPerIp = input.map(line => {

      val lineArr = line.split(" ")
      val requestTime = this._computeStamp(lineArr(0))
      val userIp = lineArr(2)

      (userIp, this._createDefautlSessionInfo(requestTime))

    }).sortBy(p => p._2.stampStart)

    // Compute the statistic for each session per user.
    var sessionsPerUser = requestsPerIp.
      reduceByKey((acc, value) => this._updateSessionInfo(acc, value))

    // At the end compute the averages as well, append them to the results,
    // and finally sort the data by longest session times to see the most
    // engaged users.
    sessionsPerUser = sessionsPerUser.map(p => (p._1, p._2.copy(count = p._2.count + 1)))
    val sessionResultsWithAverages = sessionsPerUser.map(nextPair => {

      val sessionInfo = nextPair._2
      val avg = sessionInfo.totalLength /
        sessionInfo.count

      (nextPair._1, sessionInfo, avg)
    }).sortBy(a=> a._2.longest, ascending = false)

    // Return the result.
    sessionResultsWithAverages
  }

  /**
    * Determine the unique IP visit per user.
    */
  def computeUniqueVisits(input: RDD[String]): RDD[(String, Int)] = {

    // Parse for the necessary information first.
    val infoPerUser = input.map(line => {
      val lineArr = line.split(" ")
      val userIp = lineArr(2)
      val requestAddr = lineArr(3)

      ((userIp, requestAddr), 1)
    })

    // Get the unique vists per user.
    val uniqueVistPerUser = infoPerUser.
      reduceByKey(_ + _).
      map(p => (p._1._1, 1)).
      reduceByKey(_ + _)

    // Return the result.
    uniqueVistPerUser
  }

  /**
    * Helper function to update session information
    * as part of an (accumulative, value) function.
    */
  private def _updateSessionInfo(
    acc: SessionInfo,
    value: SessionInfo): SessionInfo = {

    // Determine if there is a new user session.
    val isNewSession = this._isNewSession(acc.stampEnd, value.stampStart)
    if (isNewSession) {

      // If there is a new session, increase the session count, reset
      // the current length to zero, and keep the total session length
      // the same, as the inactive time (which was more than 15 minutes)
      // does not count as part of session activity.
      SessionInfo(
        stampStart = acc.stampStart,
        stampEnd = value.stampEnd,
        count = acc.count + value.count + 1,
        totalLength = acc.totalLength + value.totalLength,
        backSegment = acc.backSegment,
        frontSegment = value.frontSegment,
        longest = Math.max(acc.longest, value.longest))

    } else {

      // Otherwise an existing session is still taking place. In this
      // case update the current session length, update the total
      // session activity, and keep track of the longest session seen so far.
      val timeDiff = value.stampStart - acc.stampEnd

      var backSegment = acc.backSegment
      if(acc.backSegment._2 == acc.stampEnd) {
        backSegment = (acc.backSegment._1, value.backSegment._2)
      }

      var frontSegment = value.frontSegment
      if(value.frontSegment._1 == value.stampStart) {
        frontSegment = (acc.frontSegment._1, value.frontSegment._2)
      }

      val centerSegmentLength = value.backSegment._2 - acc.frontSegment._1

      SessionInfo(
        stampStart = acc.stampStart,
        stampEnd = value.stampEnd,
        count = acc.count + value.count,
        totalLength = acc.totalLength + value.totalLength + timeDiff,
        backSegment = backSegment,
        frontSegment = frontSegment,
        longest = Math.max(
          Math.max(acc.longest, value.longest),
          Math.max(backSegment._2 - backSegment._1, Math.max(frontSegment._2 - frontSegment._1, centerSegmentLength))))
    }
  }

  /**
    * Helper function to create default session info.
    */
  private def _createDefautlSessionInfo(stamp: Long): SessionInfo = {
    SessionInfo(
      stampStart = stamp,
      stampEnd = stamp,
      count = 0,
      totalLength = 0,
      backSegment = (stamp, stamp),
      frontSegment = (stamp, stamp),
      longest = 0)
  }

  /**
    * Help compute the time stamp in milliseconds.
    */
  private def _computeStamp(input: String): Long = {
    new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").
      parse(input.substring(0, 23) + "Z").getTime
  }

  /**
    * Determine if a new session started based on
    * the previous and current time stamp.
    */
  private def _isNewSession(prev: Long, curr: Long): Boolean = {
    (curr - prev) >= this._inactiveWindowMillis
  }

  /**
    * Class for helping to count sessions.
    */
  case class SessionInfo (
    stampStart: Long,
    stampEnd: Long,
    count: Int,
    totalLength: Double,
    backSegment: (Long, Long),
    frontSegment: (Long, Long),
    longest: Double)
}
