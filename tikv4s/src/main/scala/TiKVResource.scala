package tikv4s

import cats.effect.IO
import cats.effect.Resource
import cats.effect.Sync

import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;

object TiKVResource {

  def fromConf[F[_]](conf: TiConfiguration)(using F: Sync[F]): Resource[F, TiSession] = {
    Resource.make {
      F.delay(TiSession.create(conf))
    } { client =>
      F.delay(client.close())
    }
  }

  def fromSession[F[_]](session: TiSession)(using F: Sync[F]): Resource[F, RawKVClient] = {
    Resource.make {
      F.delay(session.createRawClient())
    } { client =>
      F.delay(client.close())
      // .handleErrorWith(_ => IO.unit) //fixme: エラーを握り部しているのを修正する
    }
  }

  def RawKVClientFromConf[F[_]](conf: TiConfiguration)(using F: Sync[F]): Resource[F, RawKVClient] = {
    for {
      session <- fromConf(conf)
      client  <- fromSession(session) 
    } yield client
  }
}