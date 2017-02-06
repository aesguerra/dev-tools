package org.minyodev.examples

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

/**
  * Created by aesguerra on 1/27/17.
  */
object ZkFileChecker extends App {
  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy)
  client.start()

  client.create().forPath("/home/aesguerra/zk/ingest/success", "2017012610".getBytes)
}
