package iu9.storm.lab7;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Main {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("generator",new DataSpout());

        builder.setBolt("splitter",new SplitterBolt(),10)
                .shuffleGrouping("generator","lines");

        builder.setBolt("counter",new CounterBolt(),10)
                .fieldsGrouping("splitter",new Fields("word"))
                .allGrouping("generator","sync");

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Frequency Dictionary", config,builder.createTopology());
    }
}
