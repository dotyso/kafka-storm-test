/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.iot.storm;

import java.util.ArrayList;
import java.util.List;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class MykafkaTopology {

    /**
     * @param args
     * @throws AuthorizationException
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        String topic = "test";
        ZkHosts zkHosts = new ZkHosts("192.168.142.131:2181");
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic,
                "/kafka",
                "MyTrack");
        List<String> zkServers = new ArrayList<String>();
        zkServers.add("192.168.142.131");
        spoutConfig.zkServers = zkServers;
        spoutConfig.zkPort = 2181;
        spoutConfig.socketTimeoutMs = 60 * 1000;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
        builder.setBolt("bolt1", new MyKafkaBolt(), 1).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(false);

        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }
            catch (Exception e) {
                e.printStackTrace();
            }

        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology", conf, builder.createTopology());
        }

    }

}
