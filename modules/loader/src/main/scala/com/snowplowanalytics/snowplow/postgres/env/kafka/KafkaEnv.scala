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

import cats.Monad
import cats.data.{EitherT, NonEmptyList}
import cats.effect.{Clock, ConcurrentEffect, ContextShift, Resource, Timer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.ParsingError.NotTSV
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload}
import com.snowplowanalytics.snowplow.postgres.config.Cli
import com.snowplowanalytics.snowplow.postgres.config.LoaderConfig.{Purpose, Source}
import com.snowplowanalytics.snowplow.postgres.env.Environment
import com.snowplowanalytics.snowplow.postgres.streaming.{SinkPipe, StreamSink, TimeUtils}
import fs2.kafka._
import fs2.Pipe

import java.nio.charset.StandardCharsets
//import scala.concurrent.duration._
import cats.syntax.all._

object KafkaEnv {

  def create[F[_]: ConcurrentEffect: ContextShift: Clock: Timer](/*_blocker: Blocker,*/
                                                                 config: Source.Kafka,
                                                                 badSink: StreamSink[F],
                                                                 purpose: Purpose): Resource[F, Environment[F, CommittableConsumerRecord[F, String, Array[Byte]]]] = {

    val consumerSettings =
      ConsumerSettings[F, String, Array[Byte]]
        .withBootstrapServers(config.bootstrapServers)
        .withProperties(config.consumerConf)
        .withEnableAutoCommit(false) // prevent enabling auto-commits by setting this after user-provided config
        .withProperties(
          ("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"),
          ("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        )
    val kafka = KafkaConsumer[F]
      .stream(consumerSettings)
      .subscribeTo(config.topicName)
      .records
    Resource.pure(
      Environment(
          kafka,
          badSink,
          getPayload[F](purpose, _),
          checkpointer(config),
          SinkPipe.OrderedPipe.forTransactor[F]
      )
    )
  }

  /*
  private def getSource[F[_]](kafka: KafkaConsumer[F, String, String], topicId: String): Stream[F, CommittableConsumerRecord[F, String, String]] = {
    // These arguments are used to create KinesisConsumerSettings later on.
    // However, only bufferSize field of created KinesisConsumerSettings object is used later on
    // therefore given stream name and app name are not used in anywhere.
    kafka.stream.subscribeTo(topicId)
  }
  */

  private def getPayload[F[_]: Clock: Monad](purpose: Purpose, record: CommittableConsumerRecord[F, String, Array[Byte]]): F[Either[BadRow, String]] =
    EitherT.fromEither[F](Either.catchNonFatal(new String(record.record.value, StandardCharsets.UTF_8)))
      .leftSemiflatMap[BadRow] { _ =>
        val payload = new String(record.record.value, StandardCharsets.UTF_8)
        purpose match {
          case Purpose.Enriched =>
            Monad[F].pure(BadRow.LoaderParsingError(Cli.processor, NotTSV, Payload.RawPayload(payload)))
          case Purpose.SelfDescribing =>
            TimeUtils.now[F].map { now =>
              val failure = Failure.GenericFailure(
                now,
                NonEmptyList.of("\"Cannot deserialize self-describing JSON from record\"")
              )
              BadRow.GenericError(Cli.processor, failure, Payload.RawPayload(payload))
            }
        }
      }.value

  private def checkpointer[F[_]: Timer: ConcurrentEffect](/* _kafka: KafkaConsumer[F, String, String], */ config: Source.Kafka)
    : Pipe[F, CommittableConsumerRecord[F, String, Array[Byte]], Unit] = {
    val offset: Pipe[F, CommittableConsumerRecord[F, String, Array[Byte]], CommittableOffset[F]] = _.map(_.offset)
    offset.andThen(commitBatchWithin(config.maxBatchSize, config.maxBatchWait))
  }
}
