
import org.minyodev.commons.SparkJobDriver.SparkJob
import org.minyodev.spark.jobs._
import org.apache.spark.{SparkConf, SparkContext}
import org.minyodev.spark.jobs._

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.concurrent.Future

/**
  * @author aesguerra
  */
object WorkflowLauncher extends App {

  val customArgs: Array[String] = Array("/home/aesguerra/etc/conf/a.xml")
  val sconf = new SparkConf().setAppName("WorkflowLauncher").setMaster("local[*]")
  val sc = new SparkContext(sconf)

  workflowLauncher {
    run(Job1)
    run(Job5)

    run(Job2) after Job1
    run(Job3) after Job1
    run(Job4) after Job2
    run(Job6) after (Job5, Job1)
  }

  /*
  * Recursive Tree Traversal
    1. iterate the map
    2. check if already finished, if not yet #3, else return or go to #1
    3. check all dependencies if finished, if yes execute job, if a dependency is not yet finished go to that job/step then apply #2 to #3 else go to next dependency
  * */

  // val jobs: HashMap[SparkJob, ListBuffer[SparkJob]] = new HashMap[SparkJob, ListBuffer[SparkJob]]
  val jobs = scala.collection.mutable.Set[run[SparkJob]]()

  /**
    * Represents a Job
    * @param job Given Job that extends to SparkJob class
    * @param dependencies Contains all jobs where job depends
    * */
  case class run[+T <: SparkJob](job: T) {
    val dependencies = ListBuffer[run[SparkJob]]()
    var isFinished = false
    // Check if already in HashMap
    if(!jobs.contains(this)) jobs += this

    // Put to ListBuffer its Job dependencies
    def after(rs: run[SparkJob]*): this.type = {
      rs.foreach(r => dependencies.append(r))
      this
    }
  }

  implicit def toRun[T <: SparkJob](job: T): run[T] = run[T](job)

  /** no need */
  case class FutureJobs(job: SparkJob, future: Future[Unit], jobThatDepends: SparkJob, var isFinished: Boolean = false)

  def workflowLauncher(builder: => Unit): Unit = {
    // Declaration phase
    builder

    // execution
    jobs.foreach(r => {
      if(!r.isFinished) {
        runDependencies(r)
        runJob(r)
      }
    })

  }

  def runDependencies(r: run[_]): Boolean = {
    if(r.dependencies.size == 0)
      true // no to check dependencies, maybe this is the root
    else {
      r.dependencies
        .foldLeft(true)((a, c) => {
            if(c.isFinished) a & true
            else a & { runDependencies(c) & runJob(c) }
        })
    }
  }

  def runJob(r: run[_]): Boolean = {
    if(r.isFinished) true
    else { // actual run here
      // call r.job.wrappedRun here
      // we'll just return true for now
      // and update r.isFinished = true
      r.isFinished = true
      r.isFinished
    }
  }
}
