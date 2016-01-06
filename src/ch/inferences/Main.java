package ch.inferences;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;

public class Main {

    public static void main(String[] args) {
        String[] esNodes = {"localhost:9300"};
        TransportClient esClient = esConnector.createTransportClient(esConnector.setSettings(), esNodes);

        // Get all indices in cluster
        String[] indices = esClient
                .admin()
                .indices()
                .getIndex(new GetIndexRequest())
                .actionGet()
                .indices();
        for (String i : indices) {
            System.out.println(i);
        }

        // Check if document exists
        SearchResponse response = esClient
                .prepareSearch("testsb_151230")
                .setQuery(QueryBuilders.idsQuery("person").addIds("2723e22c-40a8-32fb-9668-7028ce8dcb52"))
                .setFetchSource(false)
                .execute()
                .actionGet();
        System.out.print(response.getHits().totalHits());
    }
}
