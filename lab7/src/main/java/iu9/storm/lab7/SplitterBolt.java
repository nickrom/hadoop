package iu9.storm.lab7;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitterBolt  extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector output) {
        this.outputCollector = output;
    }

    @Override
    public void execute(Tuple input) {
        String [] words = input.getStringByField("line").split("[^a-zA-Zа-яА-Я]+");
        for (String word : words) {
            outputCollector.emit(input,new Values(word.toLowerCase()));
        }
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}