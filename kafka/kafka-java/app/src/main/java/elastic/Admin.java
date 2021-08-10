package elastic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class Admin {

    private static final String BOOTSTRAP_SERVERS_CONFIG = "";
    private static final Logger logger = LoggerFactory.getLogger(Admin.class);

    public AdminClient getAdmin()  {
        try{
            Properties configs = new Properties();
            configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
            return AdminClient.create(configs);
        }catch (Exception e) {
            return null;
        }
    }

    public void getBrokerInfo() {
        AdminClient admin = getAdmin();
        try{
            logger.info("start getBrokerInfo func");


            for (Node node: admin.describeCluster().nodes().get()){
                logger.info("node: {}", node);
                ConfigResource cr = new ConfigResource(ConfigResource.Type.BROKER, node.idString());
                DescribeConfigsResult describeConfigs = admin.describeConfigs(Collections.singleton(cr));
                describeConfigs.all().get().forEach((broker,config) -> {
                    config.entries().forEach((entry) -> logger.info(entry.name()+"= "+entry.value()));
                });
            }
        }catch (Exception e) {
            logger.info(e.getMessage());
        }
        admin.close();
    }

    public void getTopicInfo() {
        AdminClient admin = getAdmin();
        try{
            logger.info("start getBrokerInfo func");
            Map<String, TopicDescription> topicInfo = admin.describeTopics(Collections.singletonList("test")).all().get();
            logger.info("topicInfo: {}", topicInfo);
        }catch (Exception e) {
            logger.info(e.getMessage());
        }
        admin.close();
    }
}
