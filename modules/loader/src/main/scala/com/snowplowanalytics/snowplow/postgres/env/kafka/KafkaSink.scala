/*
 * Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.postgres.env.kafka

import cats.effect.{Async, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.implicits._
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig
//import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{BackoffPolicy}
import com.snowplowanalytics.snowplow.postgres.streaming.StreamSink
import org.log4s.getLogger

import scala.jdk.CollectionConverters._
import java.nio.ByteBuffer
import java.util.UUID
import fs2.kafka._
import org.apache.kafka.clients.admin.AdminClient

import scala.collection.mutable.ArrayBuffer

object KafkaSink {

  lazy val logger = getLogger

  def create[F[_]: Async: Timer: ConcurrentEffect: ContextShift](config: LoaderConfig.StreamSink.Kafka/*, backoffPolicy: BackoffPolicy*/): Resource[F, StreamSink[F]] =
    mkProducer(config).map(writeToKafka(config.topicId/*, backoffPolicy*/))

  private def mkProducer[F[_]: Sync: ConcurrentEffect: ContextShift](config: LoaderConfig.StreamSink.Kafka): Resource[F, KafkaProducer[F, String, String]] = {
    val settings = ProducerSettings[F, String, String].withBootstrapServers(config.topicId) //todo
    KafkaProducer[F].resource(settings)
  }

  private def writeToKafka[F[_]: Async: Timer: ContextShift](streamName: String/*, _backoffPolicy: BackoffPolicy*/)
                                                              (producer: KafkaProducer[F, String, String])
                                                              (data: Array[Byte]): F[Unit] = {
    val res = for {
      byteBuffer <- Async[F].delay(ByteBuffer.wrap(data))
      partitionKey <- Async[F].delay(UUID.randomUUID().toString)
      record = ProducerRecord(streamName, partitionKey, byteBuffer.toString())
      cb <- producer.produce(ProducerRecords.one(record))
//      cbRes <- registerCallback(cb)
      _ <- ContextShift[F].shift
    } yield cb
//    res.retryingOnFailuresAndAllErrors(
//      wasSuccessful = _.records.isEmpty, //??
//      policy = backoffPolicy.retryPolicy,
//      onFailure = (_, retryDetails) => Async[F].delay(logger.warn(s"Failure while writing record to Kinesis - retries so far: ${retryDetails.retriesSoFar}")),
//      onError = (exception, retryDetails) => Async[F].delay(logger.warn(s"Error while writing record to Kinesis - retries so far: ${retryDetails.retriesSoFar} - exception: $exception"))
//    ).void
    res.void
  }

//  private def registerCallback[F[_]: Async](f: F[ProducerResult[String, String, Unit]]): F[ProducerResult[String, String, Unit]] =
//    Async[F].async[ProducerResult[String, String, Unit]] { cb =>
//      Futures.addCallback(
//        f,
//        new FutureCallback[ProducerResult[String, String, Unit]] {
//          override def onFailure(t: Throwable): Unit = cb(Left(t))
//
//          override def onSuccess(result: ProducerResult[String, String, Unit]): Unit = cb(Right(result))
//        },
//        MoreExecutors.directExecutor
//      )
//    }

  def topicExists[F[_]: Async: ContextShift](config: LoaderConfig.StreamSink.Kafka)/*(implicit ec: ExecutionContext)*/: F[Boolean] =
    mkKafkaAdminClient(Map()).use { client =>
      Async[F].async[Boolean] { cb =>
        val lists = client.describeTopics(ArrayBuffer(config.topicId).asJava)
        val exist = lists.values().asScala.get(config.topicId).map(_.get).map(_.name()).nonEmpty
        cb(Right(exist))
      }.flatMap { result =>
        ContextShift[F].shift.as(result)
      }
    }

  def mkKafkaAdminClient[F[_]: Sync](props: Map[String, Object]): Resource[F, AdminClient] =
    Resource.fromAutoCloseable {
      Sync[F].delay {
        AdminClient.create(props.asJava)
      }
    }

}
