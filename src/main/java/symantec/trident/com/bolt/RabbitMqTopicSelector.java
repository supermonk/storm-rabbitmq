package symantec.trident.com.bolt;

import java.io.Serializable;

import org.apache.storm.trident.tuple.TridentTuple;

public interface RabbitMqTopicSelector extends Serializable {

  String getTopic(TridentTuple tuple);

}
