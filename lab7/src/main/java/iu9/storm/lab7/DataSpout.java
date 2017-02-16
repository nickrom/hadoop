package iu9.storm.lab7;

import com.google.common.io.Files;
import org.apache.storm.shade.com.google.common.base.Charsets;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.*;
import java.util.Map;

public class DataSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader reader;
    private File workingDirectory;
    private File currentFile;


    public DataSpout() {
        workingDirectory = new File("/home/user/hadoop/lab7/input_data/");
        reader = null;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector output) {
        this.spoutOutputCollector = output;
    }

    @Override
    public void nextTuple() {
        if (reader != null) {
            try {
                String line = reader.readLine();
                if (line != null) {
                    spoutOutputCollector.emit("lines", new Values(line));
                } else {
                        Files.move(currentFile, new File("/home/user/hadoop/lab7/output_data/" + currentFile.getName()));
                        spoutOutputCollector.emit("sync", new Values());
                        reader = null;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            File[] filesListDirectory = workingDirectory.listFiles();
            if (filesListDirectory == null || filesListDirectory.length == 0) {
                Utils.sleep(100);
            } else {
                try {
                    currentFile = filesListDirectory[0];
                    reader = new BufferedReader(new InputStreamReader(new FileInputStream(currentFile), Charsets.UTF_8));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("lines", new Fields("line"));
        outputFieldsDeclarer.declareStream("sync", new Fields());
    }
}