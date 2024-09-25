package dataset

import dataset.util.Commit.Commit

import java.text.SimpleDateFormat
import java.util.SimpleTimeZone
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
    //maps commits to the stats (flat map ignores the options that are none without needing a filter seperately)
    val stats = input.flatMap(commit => commit.stats)
    val additions = stats.map(stat => stat.additions)
    additions.sum/additions.length
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
    //config date format to hours and parse as utc time
    val dateFormat = new SimpleDateFormat("HH");
    dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "wow this time is my favourite time"))

    //select commits that have at least a single file with filename ending in .js
    val jscommits = input.filter(commit => commit.files.exists(anyFile => anyFile.filename.exists(anyFileName => anyFileName.endsWith(".js"))))

    val commitsInHoursAndCount = jscommits.flatMap{singleCommit => {
      //hour associated with this commit
      val hour: Int = dateFormat.format(singleCommit.commit.committer.date).toInt
      //count changed filess, multiple js file edits in a single commit counts as multiple
      val modifiedCount= singleCommit.files.count(file => file.filename.exists(something => something.endsWith(".js")))
      //This is what we are mapping each commit to, a tuple of the hour and the amount of commits
      Seq(Tuple2(hour, modifiedCount))
    }}

    val hourToTupleSeq = commitsInHoursAndCount.groupBy(tuple => tuple._1)
    //we are mapping the values, which are currently a sequence of tuple2s to the second value (count) and summing it up.
    val hourToTotalCommits = hourToTupleSeq.mapValues(tupleSequence => tupleSequence.map(tuple => tuple._2).sum)

    return hourToTotalCommits.maxBy(hourCountTuple => hourCountTuple._2)
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

    val authNameToCommits: Map[String, List[Commit]] = input.filter(commit => commit.url.contains(repo)).groupBy(_.commit.author.name);
    val authNametoTotalCommits: Map[String, Int] = authNameToCommits.mapValues(commitList => commitList.size)
    val topCommitPerson: (String, Int) = authNametoTotalCommits.maxBy(entry => entry._2)
    topCommitPerson


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
    //filter on 2019
    val dateFormat = new SimpleDateFormat("yyyy");
    dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "wow this time is also my favourite time"))

    val commits2019 = input.filter(eachCommit => dateFormat.format(eachCommit.commit.committer.date) == "2019")
    //group by repository
    commits2019.groupBy { commit =>
      val raw : String = commit.url;


      //from 1000commits json
      val pattern = """.*repos/([^/]+/[^/]+)/commits/.*""".r

      raw match {
        case pattern(repoName) => repoName
        case _ => ""
      }
    }.mapValues(_.size).filterKeys(key => key != "")

  }


  /** Q27 (9p)
   * Derive the 5 file types that appear most frequent in the commit logs.
   * NB!filename of a file is always defined.
   * @param input the list of commits to process.
   * @return 5 tuples containing the file extension and frequency of the most frequently appeared file types, ordered descendingly.
   */
  def topFileFormats(input: List[Commit]): List[(String, Int)] = {
    val fileTypeCounts = input.flatMap { commit =>
        commit.files.map { file =>
          val optionalFileName = file.filename.getOrElse("")
          val dotPosition = optionalFileName.lastIndexOf('.')

          dotPosition match {
            //if the optional was none:
            case -1 => ""
            //if the optional exists
            case anyPosition => optionalFileName.substring(anyPosition + 1)
          }
        }
      }//now we have a list of extensions
      //group by filenames
      .groupBy(identity)
      .mapValues(_.size) // count size of subgroup


    fileTypeCounts.toList
      .sortBy(-_._2) //sort desc
      .take(5)       //top 5
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
    val dateFormat = new SimpleDateFormat("HH")
    dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "name")) // Assuming UTC, adjust if needed

    // map commit to part of day
    val partsOfDay = input.map { eachCommit =>
      val commitHour = dateFormat.format(eachCommit.commit.committer.date).toInt

      commitHour match {
        case hour if hour >= 5 && hour < 12 => ("morning")
        case hour if hour >= 12 && hour < 17 => ("afternoon")
        case hour if hour >= 17 && hour < 21 => ("evening")
        case _ => ("night")
      }
    }
    //now we have a list of, dayparts like (e), (m), (a), (a)

    val partCounts = partsOfDay
      .groupBy(identity)
      .mapValues(entry => entry.size)
    //now we have a map of daypart to amount of commits respective

    //return the entry with the highest amount
    partCounts.maxBy(entry => entry._2)
  }
}
