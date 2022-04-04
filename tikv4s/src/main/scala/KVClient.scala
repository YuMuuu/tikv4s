package tikv4s

import cats.effect.IO
import cats.effect.kernel.Resource
import org.tikv.common.TiSession
import org.tikv.txn.{KVClient => KVClientJ}
import org.tikv.shade.com.google.protobuf.ByteString
import org.tikv.common.util.ConcreteBackOffer
import org.tikv.common.TiConfiguration
import org.tikv.common.region.RegionStoreClient.RegionStoreClientBuilder
import org.tikv.common.region.RegionStoreClient
import org.tikv.common.exception.KeyException
import org.tikv.common.exception.TiKVException
import cats.data.Op
import org.tikv.common.util.BackOffFunction
import org.tikv.common.meta.TiTimestamp




case class KVClient(
  conf: TiConfiguration , 
  clientBuilder: RegionStoreClientBuilder 
) {
  private def regionStoreClient(key: ByteString): Resource[IO, RegionStoreClient] = 
    Resource.make(IO(clientBuilder.build(key)))(client => IO(client.close))

  def get(key: ByteString, version: TiTimestamp): IO[Option[ByteString]] = {
    val backOffer = ConcreteBackOffer.newGetBackOff()
    regionStoreClient(key).use { client => {
        val io = IO(client.get(backOffer, key, version.getVersion))
        io.map(Some(_)).handleErrorWith {
          case _: KeyException => IO.pure(None)
          case e: TiKVException => 
            //失敗しまくるとthread使い潰す...?
            IO{backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e)} *> get(key, version)
          case e: Exception => IO.raiseError(e)
        }
      }
    }
  }

  //batchGet
  //scan
  //も作る


}