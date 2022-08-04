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
import fs2.{Pipe, Stream}
//import scala.concurrent.duration._
import cats.syntax.all._

object KafkaEnv {

  def create[F[_]: ConcurrentEffect: ContextShift: Clock: Timer](/*_blocker: Blocker,*/
                                                                 config: Source.Kafka,
                                                                 badSink: StreamSink[F],
                                                                 purpose: Purpose): Resource[F, Environment[F, CommittableConsumerRecord[F, String, String]]] = {

    for {
      kafka <- KafkaConsumer[F].resource(config.settings.unwrap)
    } yield Environment(
        getSource(kafka),
        badSink,
        getPayload[F](purpose, _),
        checkpointer(config.settings),
        SinkPipe.OrderedPipe.forTransactor[F]
      )
  }

  private def getSource[F[_]](kafka: KafkaConsumer[F, String, String]): Stream[F, CommittableConsumerRecord[F, String, String]] = {
    // These arguments are used to create KinesisConsumerSettings later on.
    // However, only bufferSize field of created KinesisConsumerSettings object is used later on
    // therefore given stream name and app name are not used in anywhere.
    kafka.stream
  }

  private def getPayload[F[_]: Clock: Monad](purpose: Purpose, record: CommittableConsumerRecord[F, String, String]): F[Either[BadRow, String]] =
    EitherT.fromEither[F](Either.catchNonFatal(record.record.value))
      .leftSemiflatMap[BadRow] { _ =>
        val payload = record.record.value
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

  private def checkpointer[F[_]: Timer: ConcurrentEffect](/* _kafka: KafkaConsumer[F, String, String], */ settings: Source.Kafka.Settings)
    : Pipe[F, CommittableConsumerRecord[F, String, String], Unit] = {
    val offset: Pipe[F, CommittableConsumerRecord[F, String, String], CommittableOffset[F]] = _.map(_.offset)
    offset.andThen(commitBatchWithin(settings.maxBatchSize, settings.maxBatchWait))
  }
}
