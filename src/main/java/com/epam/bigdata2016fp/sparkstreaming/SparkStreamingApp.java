package com.epam.bigdata2016fp.sparkstreaming;

import com.epam.bigdata2016fp.sparkstreaming.conf.AppProperties;
import com.epam.bigdata2016fp.sparkstreaming.hbase.HbaseProcessor;
import com.epam.bigdata2016fp.sparkstreaming.kafka.KafkaProcessor;
import com.epam.bigdata2016fp.sparkstreaming.model.CityInfo;
import com.epam.bigdata2016fp.sparkstreaming.model.ESModel;
import com.epam.bigdata2016fp.sparkstreaming.model.LogLine;
import com.epam.bigdata2016fp.sparkstreaming.utils.DictionaryUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import java.util.Map;


@ComponentScan
@EnableAutoConfiguration
public class SparkStreamingApp {

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(SparkStreamingApp.class).run(args);
        AppProperties props = ctx.getBean(AppProperties.class);
        JavaStreamingContext jsc = ctx.getBean(JavaStreamingContext.class);

        Map<String, CityInfo> dict1 = DictionaryUtils.citiesDictionry(props.getHadoop());
        Broadcast<Map<String, CityInfo>> brCitiesDict = jsc.sparkContext().broadcast(dict1);

        Map<String, String> dict2 = DictionaryUtils.tagsDictionry(props.getHadoop());
        Broadcast<Map<String, String>> brTagsDict = jsc.sparkContext().broadcast(dict2);

        LogisticRegressionModel model1 = LogisticRegressionModel.load(jsc.sparkContext().sc(), "tmp/fp/ml");
        //DecisionTreeModel tree = DecisionTreeModel.load(jsc.sparkContext().sc(), "tmp/fp/ml");
        /*double result = tree.predict(Vectors.dense(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0));
        System.out.println("####123");
        System.out.println(result);*/



        JavaPairReceiverInputDStream<String, String> logs =
                KafkaProcessor.getStream(jsc, props.getKafkaConnection());

        //save to ELASTIC SEARCH
        String index = props.getElasticSearch().getIndex();
        String type = props.getElasticSearch().getType();
        String confStr = index + "/" + type;
        logs.map(keyValue -> {

            /*double result2 = tree.predict(Vectors.dense(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0));
            System.out.println("####123");
            System.out.println(result2);*/

            ESModel model = ESModel.parseLine(keyValue._2());
            CityInfo cityInfo = brCitiesDict.value().get(Integer.toString(model.getCity()));
            model.setGeoPoint(cityInfo);
            model.setMlResult(model1.predict(Vectors.dense(model.getOsName().hashCode(),
                    model.getDevice().hashCode(),
                    model.getUaFamily().hashCode(),
                    model.getRegion(),
                    model.getCity(),
                    model.getDomain().hashCode(),
                    model.getAddSlotWidth(),
                    model.getAddSlotHeight(),
                    model.getAddSlotVisability(),
                    model.getAddSlotFormat(),
                    brTagsDict.value().get(model.getUserTags()).split(",")[0].hashCode())));
            //double result2 = tree.predict(Vectors.dense(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0));
            return model;
        }).map(ESModel::toStringifyJson).foreachRDD(jsonRdd -> JavaEsSpark.saveJsonToEs(jsonRdd, confStr));


        //save to HBASE
        /*JavaDStream<LogLine> logLineStream = logs.map(keyValue -> LogLine.parseLogLine(keyValue._2()));
        logLineStream
                .filter(line -> !"null".equals(line.getiPinyouId()))
                .foreachRDD(rdd ->
                        rdd.map(line -> {
                            CityInfo cityInfo = brCitiesDict.value().get(Integer.toString(line.getCity()));
                            line.setGeoPoint(cityInfo);
                            String tags = brTagsDict.value().get(line.getUserTags());
                            line.setTagsList(tags);
                            Put put = LogLine.convertToPut(line, props.getHbase().getColumnFamily());
                            return put;
                        }).foreachPartition(iter -> HbaseProcessor.saveToTable(iter, props.getHbase()))
                );*/


        jsc.start();
        jsc.awaitTermination();
    }

}