
import org.minyodev.commons.SparkJobDriver.SparkJob
import com.minyodev.spark.jobs._
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

  val jobs: HashMap[SparkJob, ListBuffer[SparkJob]] = new HashMap[SparkJob, ListBuffer[SparkJob]]

  /**
    * Represents a Job
    * @param job Given Job that extends to SparkJob class
    * @param dependencies Contains all jobs where job depends
    * */
  case class run[T <: SparkJob](job: T, dependencies: ListBuffer[SparkJob] = ListBuffer[SparkJob]()) {
    // Check if already in HashMap
    if(!jobs.contains(job)) jobs.put(job, dependencies)

    // Put to ListBuffer its Job dependencies
    def after(s: SparkJob*) {
      s.foreach(n => dependencies.append(n))
    }
  }

  case class FutureJobs(job: SparkJob, future: Future[Unit], jobThatDepends: SparkJob, var isFinished: Boolean = false)

  def workflowLauncher(builder: => Unit): Unit = {
    // Declaration phase
    builder

    // Preparation phase
    val futures = jobs.flatMap(kv => {
      if(kv._2.size > 0) {
        kv._2.map(dpndcs => {
          println(kv._1.name + " has dependencies with " + kv._2.mkString(","))
          val future = Future { dpndcs.wrappedRun(sc) }
          FutureJobs(dpndcs, future, kv._1)
        })
      }
      else {
        println("Running " + kv._1.name + "...")
        val future = Future { kv._1.wrappedRun(sc) }
        List(FutureJobs(kv._1, future, null))
      }
    })

    /// ----Error na below
    futures foreach(j => {
      j
      // If this job is a main job
      if(j.jobThatDepends == null) {
        if(j.future.isCompleted) {
          println(j.job.name + " is completed!")
          j.isFinished = true
        }
        else println(j.job.name + " contains errors.")
      }
      else {
        // If job contains dependency
        // Dunno what to put here........
//        if(j.isFinished == true){
//          if(j.future.isCompleted) {
//            println(j.job.name + " is completed!")
//            j.isFinished = true
//          }
//          else println(j.job.name + " contains errors.")
//        }
//        else {
//
//        }
      }
    })
  }

}