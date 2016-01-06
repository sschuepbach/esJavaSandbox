package ch.inferences;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p/>
 *          Created on 06.01.16
 */
public class esConnector {

    static public Settings setSettings() {
        return Settings.settingsBuilder()
                .put("cluster.name", "elasticsearch")
                .build();
    }

    static public TransportClient createTransportClient(Settings settings, String[] esNodes) {
        TransportClient esClient = TransportClient.builder().settings(settings).build();
        for (String elem: esNodes) {
            String[] node = elem.split(":");
            try {
                esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(node[0]), Integer.parseInt(node[1])));
            } catch (UnknownHostException e) {
                System.out.println("Some errors occurred" + e.getMessage());
            }
        }
        return esClient;
    }

}
