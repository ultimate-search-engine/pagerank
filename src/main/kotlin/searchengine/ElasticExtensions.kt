package searchengine

import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.SearchResponse
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import libraries.Elastic
import libraries.Page


// extension method on Elastic
suspend fun Elastic.getGlobalSinkRank(): Double = coroutineScope {
    val search: SearchResponse<Page.PageType> = withContext(Dispatchers.Default) {
        search(SearchRequest.of {
            it.index(index)
            it.query { query ->
                query.bool { bool ->
                    bool.mustNot { mustNot ->
                        mustNot.exists { exists ->
                            exists.field("body.links.internal")
                        }
                        mustNot.exists { exists ->
                            exists.field("body.links.external")
                        }
                    }
                }
            }
            it.aggregations("total") { agg ->
                agg.sum { sum ->
                    sum.field("inferredData.ranks.pagerank")
                }
            }
        })
    }
    return@coroutineScope search.aggregations()["total"]?.sum()?.value() ?: 0.0

}
