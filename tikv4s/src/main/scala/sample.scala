package tikv4s

import scala.util.Try
import scala.util.{Try, Success, Failure}

import cats.free.Free

import org.tikv.common.BytePairWrapper;
import org.tikv.common.ByteWrapper;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Kvrpcpb.KvPair;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.KVClient;
import org.tikv.txn.TwoPhaseCommitter;

import collection.JavaConverters._


// javaのサンプルコードをscalaでそのまま書き写したもの
object sample {
  val session: TiSession = ???
  val startTS: Long = session.getTimestamp().getVersion();

  val twoPhaseCommitter: TwoPhaseCommitter = new TwoPhaseCommitter(session, startTS)

  val backOffer: BackOffer  = ConcreteBackOffer.newCustomBackOff(1000);
  val primaryKey: Array[Byte]  = "key1".getBytes("UTF-8");
  val key2: Array[Byte] = "key2".getBytes("UTF-8");

  // first phrase: prewrite
  twoPhaseCommitter.prewritePrimaryKey(backOffer, primaryKey, "val1".getBytes("UTF-8"))
  val pairs: List[BytePairWrapper] = List(new BytePairWrapper(key2, "val2".getBytes("UTF-8")))
  twoPhaseCommitter.prewriteSecondaryKeys(primaryKey, pairs.iterator.asJava, 1000)

  // second phrase: commit
  val  commitTS: Long = session.getTimestamp().getVersion()
  twoPhaseCommitter.commitPrimaryKey(backOffer, primaryKey, commitTS)
  val keys: List[ByteWrapper] = List(new ByteWrapper(key2))
  twoPhaseCommitter.commitSecondaryKeys(keys.iterator.asJava, commitTS, 1000)
}