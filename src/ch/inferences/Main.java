package ch.inferences;

import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import static org.elasticsearch.index.query.QueryBuilders.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

        // Get all types for all indices
/*        for (String i: indices) {

            String[] types = esClient
                    .
                    .prepareSearch(i)
                    .setQuery(QueryBuilders.)
        }*/

        BoolQueryBuilder complexQuery = new BoolQueryBuilder()
                .must(matchQuery("foaf:firstName", "Nele"))
                .must(matchQuery("foaf:lastName", "Neuhaus"));
        SearchResponse matchResponse = esClient
                .prepareSearch("testsb_151230")
                .setTypes("person")
                .setQuery(complexQuery)
                .execute()
                .actionGet();
        Map test = matchResponse.getHits().getAt(0).getSource();
        System.out.println(matchResponse.getHits().getTotalHits());


        // Get all documents in type
        SearchResponse scrollResponse = esClient
                .prepareSearch("testsb_151230")
                .setTypes("person")
                .setScroll(new TimeValue(10000))
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(10000)
                .setFetchSource(false)
                .execute()
                .actionGet();
        List<String> hits = new ArrayList<>();
        while (true) {
            for (SearchHit hit : scrollResponse.getHits().getHits()) {
                hits.add(hit.getId());
            }
            // Initiate new scrolling
            scrollResponse = esClient
                    .prepareSearchScroll(scrollResponse.getScrollId())
                    .setScroll(new TimeValue(10000))
                    .execute()
                    .actionGet();
            // Break if no documents left
            if (scrollResponse.getHits().getHits().length == 0) break;
        }
        System.out.println(hits.size());
        int x = 1;
        for (String h: hits) {
            SearchResponse response = esClient
                    .prepareSearch("testsb_151230")
                    .setSearchType(SearchType.QUERY_THEN_FETCH)         // Default value
                    .setSize(0)
                    .setQuery(QueryBuilders.idsQuery("person").addIds(h))
                    .execute()
                    .actionGet();
            String output = Integer.toString(x);
            output += response.getHits().totalHits() >= 1 ? ": exists " : ": does not exist";
            System.out.println(output);
            x++;
        }

        // Check if document exists
        SearchResponse response = esClient
                .prepareSearch("testsb_151230")
                .setSearchType(SearchType.QUERY_THEN_FETCH)         // Default value
                .setSize(0)
                .setQuery(QueryBuilders.idsQuery("person").addIds("2723e22c-40a8-32fb-9668-7028ce8dcb52"))
                // .setFetchSource(false)
                .execute()
                .actionGet();
        System.out.print(response.getHits().totalHits());
    }
}
