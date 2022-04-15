package searchengine

import libraries.Address
import libraries.Credentials
import libraries.Elastic

class ElasticIndexer(private val pagerank: Pagerank) {
    val elastic = Elastic(Credentials("", ""), Address("localhost", 9200), "experimental")





}