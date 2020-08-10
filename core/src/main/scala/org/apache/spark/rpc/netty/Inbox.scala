/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rpc.netty

import javax.annotation.concurrent.GuardedBy

import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, ThreadSafeRpcEndpoint}


private[netty] sealed trait InboxMessage

private[netty] case class OneWayMessage(
    senderAddress: RpcAddress,
    content: Any) extends InboxMessage

private[netty] case class RpcMessage(
    senderAddress: RpcAddress,
    content: Any,
    context: NettyRpcCallContext) extends InboxMessage

private[netty] case object OnStart extends InboxMessage

private[netty] case object OnStop extends InboxMessage

/** A message to tell all endpoints that a remote process has connected. */
private[netty] case class RemoteProcessConnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a remote process has disconnected. */
private[netty] case class RemoteProcessDisconnected(remoteAddress: RpcAddress) extends InboxMessage

/** A message to tell all endpoints that a network error has happened. */
private[netty] case class RemoteProcessConnectionError(cause: Throwable, remoteAddress: RpcAddress)
  extends InboxMessage

/**
 * An inbox that stores messages for an [[RpcEndpoint]] and posts messages to it thread-safely.
 */
private[netty] class Inbox(val endpointName: String, val endpoint: RpcEndpoint)
  extends Logging {

  inbox =>  // Give this an alias so we can use it more clearly in closures.

  @GuardedBy("this")
  protected val messages = new java.util.LinkedList[InboxMessage]()

  /** True if the inbox (and its associated endpoint) is stopped. */
  @GuardedBy("this")
  private var stopped = false

  /** Allow multiple threads to process messages at the same time. */
  @GuardedBy("this")
  private var enableConcurrent = false

  /** The number of threads processing messages for this inbox. */
  @GuardedBy("this")
  private var numActiveThreads = 0

  // OnStart should be the first message to process
  inbox.synchronized {
    // scalastyle:off
    println(s"Add message ($endpointName): OnStart")
    messages.add(OnStart)
  }

  /**
   * Process stored messages.
   */
  def process(dispatcher: Dispatcher): Unit = {
    var message: InboxMessage = null
    // scalastyle:off
    println(s"${Thread.currentThread().getName} Inbox: ${endpointName} requiring lock(2)")
    inbox.synchronized {
      if (!enableConcurrent && numActiveThreads != 0) {
        return
      }
      message = messages.poll()
      // scalastyle:off
      import scala.collection.JavaConverters._
      println(s"${Thread.currentThread().getName} Inbox: ${endpointName} poll(1) $message")
      println(s"${Thread.currentThread().getName} Inbox: ${endpointName} Rest Message\n: ${messages.asScala.mkString("\n")}")
      if (message != null) {
        numActiveThreads += 1
      } else {
        return
      }
    }
    while (true) {
      safelyCall(endpoint) {
        message match {
          case RpcMessage(_sender, content, context) =>
            try {
              // scalastyle:off
              println(s"START ===>>> ${Thread.currentThread().getName} Inbox: ${endpointName}, RpcMessage: $content, Sender: ${_sender}")
              endpoint.receiveAndReply(context).applyOrElse[Any, Unit](content, { msg =>
                throw new SparkException(s"Unsupported message $message from ${_sender}")
              })
              println(s"END ===>>> ${Thread.currentThread().getName} Inbox: ${endpointName}, RpcMessage: $content, sender: ${_sender}")
            } catch {
              case e: Throwable =>
                context.sendFailure(e)
                // Throw the exception -- this exception will be caught by the safelyCall function.
                // The endpoint's onError function will be called.
                throw e
            }

          case OneWayMessage(_sender, content) =>
            // scalastyle:off
            println(s"START ===>>> ${Thread.currentThread().getName} Inbox: ${endpointName}, OneWayMessage: $content, Sender: ${_sender}")
            // scalastyle:on
            endpoint.receive.applyOrElse[Any, Unit](content, { msg =>
              // scalastyle:off
              println(s"Unsupported message $message from ${_sender}")
              // scalastyle:on
              throw new SparkException(s"Unsupported message $message from ${_sender}")
            })
            // scalastyle:off
            println(s"END ===>>> ${Thread.currentThread().getName} Inbox: ${endpointName}, OneWayMessage: $content, Sender: ${_sender}")

          case OnStart =>
            // scalastyle:off
            println(s"START ===>>> ${Thread.currentThread().getName} Inbox: ${endpointName}, OnStart")
            endpoint.onStart()
            if (!endpoint.isInstanceOf[ThreadSafeRpcEndpoint]) {
              inbox.synchronized {
                if (!stopped) {
                  enableConcurrent = true
                  println(s"${Thread.currentThread().getName} Inbox: ${endpointName}, enableConcurrent = true")
                }
              }
            }
            // scalastyle:off
            println(s"END ===>>> ${Thread.currentThread().getName} Inbox: ${endpointName}, OnStart")

          case OnStop =>
            val activeThreads = inbox.synchronized { inbox.numActiveThreads }
            assert(activeThreads == 1,
              s"There should be only a single active thread but found $activeThreads threads.")
            dispatcher.removeRpcEndpointRef(endpoint)
            endpoint.onStop()
            assert(isEmpty, "OnStop should be the last message")

          case RemoteProcessConnected(remoteAddress) =>
            endpoint.onConnected(remoteAddress)

          case RemoteProcessDisconnected(remoteAddress) =>
            endpoint.onDisconnected(remoteAddress)

          case RemoteProcessConnectionError(cause, remoteAddress) =>
            endpoint.onNetworkError(cause, remoteAddress)
        }
      }

      // scalastyle:off
      println(s"${Thread.currentThread().getName} Inbox: ${endpointName} requiring lock(2)")
      inbox.synchronized {
        // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
        // every time.
        if (!enableConcurrent && numActiveThreads != 1) {
          // If we are not the only one worker, exit
          numActiveThreads -= 1
          return
        }
        // scalastyle:off
        message = messages.poll()
        import scala.collection.JavaConverters._
        println(s"${Thread.currentThread().getName} Inbox: ${endpointName} poll(2) $message")
        println(s"${Thread.currentThread().getName} Inbox: ${endpointName} Rest Messages\n: ${messages.asScala.mkString("\n")}")
        if (message == null) {
          numActiveThreads -= 1
          return
        }
      }
    }
  }

  def post(message: InboxMessage): Unit = inbox.synchronized {
    if (stopped) {
      // We already put "OnStop" into "messages", so we should drop further messages
      onDrop(message)
    } else {
      // scalastyle:off
      println(s"Add message ($endpointName): $message")
      messages.add(message)
      import scala.collection.JavaConverters._
      println(messages.asScala.mkString(", "))
      false
    }
  }

  def stop(): Unit = inbox.synchronized {
    // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
    // message
    if (!stopped) {
      // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
      // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
      // safely.
      enableConcurrent = false
      stopped = true
      messages.add(OnStop)
      // Note: The concurrent events in messages will be processed one by one.
    }
  }

  def isEmpty: Boolean = inbox.synchronized { messages.isEmpty }

  /**
   * Called when we are dropping a message. Test cases override this to test message dropping.
   * Exposed for testing.
   */
  protected def onDrop(message: InboxMessage): Unit = {
    logWarning(s"Drop $message because endpoint $endpointName is stopped")
  }

  /**
   * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
   */
  private def safelyCall(endpoint: RpcEndpoint)(action: => Unit): Unit = {
    try action catch {
      case NonFatal(e) =>
        try endpoint.onError(e) catch {
          case NonFatal(ee) =>
            if (stopped) {
              logDebug("Ignoring error", ee)
            } else {
              logError("Ignoring error", ee)
            }
        }
    }
  }

}
