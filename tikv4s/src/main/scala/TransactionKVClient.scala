package tikv4s

import org.tikv.common.TiConfiguration
import org.tikv.common.region.RegionStoreClient
import org.tikv.common.ReadOnlyPDClient
import org.tikv.common.meta.TiTimestamp
import cats.effect.IO
import cats.effect.kernel.Resource
import org.tikv.common.region.TiRegion
import org.tikv.common.region.TiStore
import org.tikv.common.util.BackOffer
import org.tikv.kvproto.Kvrpcpb
import org.tikv.shade.com.google.protobuf.ByteString

import collection.JavaConverters._
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.common.exception.TiKVException
import org.tikv.common.util.BackOffFunction
import org.tikv.common.exception.GrpcException


case class TransactionKVClient(
    conf: TiConfiguration,
    clientBuilder: RegionStoreClient.RegionStoreClientBuilder,
    pdClient: ReadOnlyPDClient
) {
  // memo: timestampの取得をIOで包んで嬉しい事あるか...?
  def getTimeStamp(): IO[TiTimestamp] = {
    val bo = ConcreteBackOffer.newTsoBackOff();
    IO.blocking {
      pdClient.getTimestamp(bo)
    }.handleErrorWith { error =>
      error match {
        case e: TiKVException => {
          //memo: doBackOffの実装がよくわからない。このままだとretryまでsleepしなさそう
          IO { bo.doBackOff(BackOffFunction.BackOffFuncType.BoPDRPC, e) } *> getTimeStamp()
        }
        case e: GrpcException => {
          // logを吐く
          IO.raiseError(e)
        }
      }
    }
  }

  def prewrite(
      backOffer: BackOffer,
      mutations: List[Kvrpcpb.Mutation],
      primary: ByteString,
      lockTTL: Long,
      startTs: TiTimestamp,
      tiRegion: TiRegion,
      store: TiStore
  ): IO[Unit] = {
    regionStoreClient(tiRegion, store).use { client =>
      IO.blocking {
        client.prewrite(backOffer, primary, mutations.asJava, startTs.getVersion, lockTTL)
      }
    }
  }

  def txnHeartBeat(
      backOffer: BackOffer,
      primaryLock: ByteString,
      startTs: TiTimestamp,
      ttl: Long,
      tiRegion: TiRegion,
      store: TiStore
  ): IO[Unit] = {
    regionStoreClient(tiRegion, store).use { client =>
      IO.blocking(client.txnHeartBeat(backOffer, primaryLock, startTs.getVersion, ttl))
    }
  }

  def commit(
      backOffer: BackOffer,
      keys: List[ByteString],
      startTs: TiTimestamp,
      commitTs: TiTimestamp,
      tiRegion: TiRegion,
      store: TiStore
  ): IO[Unit] = {
    regionStoreClient(tiRegion, store).use { client =>
      IO.blocking { client.commit(backOffer, keys.asJava, startTs.getVersion, commitTs.getVersion) }
    }
  }

  private def regionStoreClient(
      tiRegion: TiRegion,
      store: TiStore
  ): Resource[IO, RegionStoreClient] = Resource.fromAutoCloseable(
    IO.blocking { clientBuilder.build(tiRegion, store) }
  )
}

//memo: 内部で使われているgrpcの形式が充分簡単なのでscalaapbでジェネレートしたほうが良い？
