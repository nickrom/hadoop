package iu9.storm.lab7;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CounterBolt extends BaseRichBolt {

    private Map<String,Integer> frequencyDict;

    private static void printToFile(String key, Integer value) {
        try(FileWriter writer = new FileWriter("/home/user/hadoop/lab7/output_data/output_lab7.txt", true)){
            writer.write(key + " : " + value + '\n');
            writer.flush();
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector output) {
        frequencyDict = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals("sync")) {
            frequencyDict.forEach(CounterBolt::printToFile);
            frequencyDict.clear();
        }
        else {
            String word = input.getStringByField("word");
            int count = frequencyDict.getOrDefault(word, 0);
            frequencyDict.put(word, count + 1);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}