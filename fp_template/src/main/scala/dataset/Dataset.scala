package dataset

import dataset.util.Commit.{Commit, CommitData, Parent}

import java.text.SimpleDateFormat
import java.util.{Calendar, SimpleTimeZone}
import scala.math.Ordering.Implicits._

/**
 * Use your knowledge of functional programming to complete the following functions.
 * You are recommended to use library functions when possible.
 *
 * The data is provided as a list of `Commit`s. This case class can be found in util/Commit.scala.
 * When asked for dates, use the `commit.commit.committer.date` field.
 *
 * This part is worth 40 points.
 */
object Dataset {


  /** Q23 (4p)
   * For the commits that are accompanied with stats data, compute the average of their additions.
   * You can assume a positive amount of usable commits is present in the data.
   *
   * @param input the list of commits to process.
   * @return the average amount of additions in the commits that have stats data.
   */
  def avgAdditions(input: List[Commit]): Int = {
    input.map(_.stats.get.additions).sum / input.size
  }

  /** Q24 (4p)
   * Find the hour of day (in 24h notation, UTC time) during which the most javascript (.js) files are changed in commits.
   * The hour 00:00-00:59 is hour 0, 14:00-14:59 is hour 14, etc.
   * NB!filename of a file is always defined.
   * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
   *
   * @param input list of commits to process.
   * @return the hour and the amount of files changed during this hour.
   */
  def jsTime(input: List[Commit]): (Int, Int) = {
    val formatter = new SimpleDateFormat("HH")
    val timezone = new SimpleTimeZone(0, "UTC")
    formatter.setTimeZone(timezone)

    // Process the commits, group by hour, and sum up the JavaScript file counts
    val times = input
      .flatMap { commit =>
        val hour = formatter.format(commit.commit.committer.date).toInt
        val jsFileCount = commit.files.count(file => file.filename.exists(_.endsWith(".js")))
        if (jsFileCount > 0) Some(hour -> jsFileCount) else None
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)  // Sum the JS file counts for each hour
      .withDefaultValue(0)         // Default value of 0 for hours with no JS files

    // Find the hour with the maximum number of JS files
    val (maxHour, maxCount) = times.maxBy(_._2)
    (maxHour, maxCount)
  }

  /** Q25 (5p)
   * For a given repository, output the name and amount of commits for the person
   * with the most commits to this repository.
   * For the name, use `commit.commit.author.name`.
   *
   * @param input the list of commits to process.
   * @param repo  the repository name to consider.
   * @return the name and amount of commits for the top committer.
   */
  def topCommitter(input: List[Commit], repo: String): (String, Int) = {
    input
      .filter(c => c.url.contains(repo))
      .groupBy(_.commit.author.name)
      .map { case (author, commits) => (author, commits.size) }
      .maxBy(_._2)
  }
  /** Q26 (9p)
   * For each repository, output the name and the amount of commits that were made to this repository in 2019 only.
   * Leave out all repositories that had no activity this year.
   *
   * @param input the list of commits to process.
   * @return a map that maps the repo name to the amount of commits.
   *
   *         Example output:
   *         Map("KosDP1987/students" -> 1, "giahh263/HQWord" -> 2)
   */
  def commitsPerRepo(input: List[Commit]): Map[String, Int] = {
    def repoName(url: String): String = url.split("/").toList match {
      case _ :: _ :: _ :: "repos" :: x :: y :: "commits" :: _ => s"$x/$y"
    }

    val formatter = new SimpleDateFormat("YYYY")
    val timezone = new SimpleTimeZone(0, "UTC")
    formatter.setTimeZone(timezone)

    input
      .filter(commit => formatter.format(commit.commit.committer.date).toInt == 2019)
      .map(commit => repoName(commit.url))
      .groupBy(identity)
      .mapValues(_.size)
  }

  /** Q27 (9p)
   * Derive the 5 file types that appear most frequent in the commit logs.
   * NB!filename of a file is always defined.
   * @param input the list of commits to process.
   * @return 5 tuples containing the file extension and frequency of the most frequently appeared file types, ordered descendingly.
   */
  def topFileFormats(input: List[Commit]): List[(String, Int)] = {
    def filetype(filename: String): String = {
      val spl = filename.split("\\.")
      if (spl.length > 1) spl.last else "file"
    }

    input
      .flatMap(_.files.map(_.filename.get))
      .map(filetype)
      .filter(_ != "file")
      .groupBy(identity)
      .mapValues(_.size)
      .toList
      .sortBy(-_._2)
      .take(5)
  }

  /** Q28 (9p)
   *
   * A day has different parts:
   * morning 5 am to 12 pm (noon)
   * afternoon 12 pm to 5 pm.
   * evening 5 pm to 9 pm.
   * night 9 pm to 4 am.
   *
   * Which part of the day was the most productive in terms of commits ?
   * Return a tuple with the part of the day and the number of commits
   *
   * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
   */
  def mostProductivePart(input: List[Commit]): (String, Int) = {
    val formatter = new SimpleDateFormat("HH")
    val timezone = new SimpleTimeZone(0, "UTC")
    formatter.setTimeZone(timezone)

    def timeOfDay(c: Commit, start: Int, end: Int): Boolean = formatter.format(c.commit.committer.date).toInt match {
      case x if x >= start && x < end => true
      case _ => false
    }

    def sumOfTime(start: Int, end: Int, input: List[Commit]): Int =
      input.count(c => timeOfDay(c, start, end))

    val morning = sumOfTime(5, 12, input)
    val afternoon = sumOfTime(12, 17, input)
    val evening = sumOfTime(17, 21, input)
    val night = sumOfTime(21, 24, input) + sumOfTime(0, 5, input)

    List(("morning", morning), ("afternoon", afternoon), ("evening", evening), ("night", night))
      .maxBy(_._2)
  }
}
