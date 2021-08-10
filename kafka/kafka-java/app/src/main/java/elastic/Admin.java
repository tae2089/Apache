package elastic;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class Admin {

    private static final String BOOTSTRAP_SERVERS_CONFIG = "";
    private static final Logger logger = LoggerFactory.getLogger(Admin.class);

    public AdminClient getAdmin() throws ExecutionException, InterruptedException {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);

        return AdminClient.create(configs);
    }

    public void getBrokerInfo() {
        try{
            logger.info("start getBrokerInfo func");
            AdminClient admin = getAdmin();
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


    }

}
