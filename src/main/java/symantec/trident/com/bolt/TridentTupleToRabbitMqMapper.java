package symantec.trident.com.bolt;

import java.io.Serializable;

import org.apache.storm.trident.tuple.TridentTuple;

public interface TridentTupleToRabbitMqMapper<K,V>  extends Serializable {
    K getKeyFromTuple(TridentTuple tuple);
    V getMessageFromTuple(TridentTuple tuple);
}
