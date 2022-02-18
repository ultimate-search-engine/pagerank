package searchengine.elastic

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.*
import co.elastic.clients.elasticsearch.core.search.Hit
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.rest_client.RestClientTransport
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient
import searchengine.types.CrawlerStatus
import searchengine.types.PageType


class Elastic(private val elasticIndex: String? = null) {
    private val client: ElasticsearchClient

    init {
        val credentialsProvider: CredentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(
            AuthScope.ANY, UsernamePasswordCredentials("elastic", "testerino")
        )

        val restClient = RestClient.builder(
            HttpHost("localhost", 9200)
        ).setHttpClientConfigCallback { httpClientBuilder ->
            httpClientBuilder.disableAuthCaching()
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        }.build()

        val transport: ElasticsearchTransport = RestClientTransport(
            restClient, JacksonJsonpMapper()
        )
        client = ElasticsearchClient(transport)
    }

    suspend fun indexPage(page: PageType, id: String? = null): IndexResponse =
        coroutineScope {
            val indexRequest = IndexRequest.of<PageType> {
                if (id != null) it.id(id)
                it.index(elasticIndex)
                it.document(page)
            }
            val res = async { client.index(indexRequest) }
            res.await()
        }

    suspend fun updatePagerankDocument(id: String, pagerank: Int) {
//        val updateRequest = UpdateRequest.of<PageType> {
//            it.index(elasticIndex)
//            it.id(id)
//            it.document(PageType(pagerank = pagerank))
//        }
//
//        Unit.apply { client.update(updateRequest) }
    }

    suspend fun docsByUrlOrNull(url: String): List<Hit<PageType>>? = coroutineScope {
        val search2: SearchResponse<PageType> = withContext(Dispatchers.Default) {
            client.search({ s: SearchRequest.Builder ->
                s.index(elasticIndex).query { query ->
                    query.term { term ->
                        term.field("address.url").value { value ->
                            value.stringValue(url)
                        }
                    }
                }
            }, PageType::class.java)
        }
        val hits = search2.hits().hits()
        return@coroutineScope hits.ifEmpty { null }
    }


    suspend fun getDocsThatDoesNotMatchComputeRoundId(computeId: Long, batchSize: Long = 200): List<Hit<PageType>> = coroutineScope {
        val search: SearchResponse<PageType> = withContext(Dispatchers.Default) {
            client.search(SearchRequest.of {
                it.index(elasticIndex)
                it.batchedReduceSize(batchSize)
                it.query { query ->
                    query.bool { bool ->
                        bool.mustNot { mustNot ->
                            mustNot.term { term ->
                                term.field("inferredData.ranks.computeRoundId").value { value ->
                                    value.longValue(computeId)
                                }
                            }
                        }
                    }
                }
            }, PageType::class.java)
        }
        return@coroutineScope search.hits().hits()
    }

    suspend fun getGlobalSinkRank(): Double = coroutineScope {
        val search: SearchResponse<PageType> = withContext(Dispatchers.Default) {
            client.search(SearchRequest.of {
                it.index(elasticIndex)
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
            }, PageType::class.java)
        }
        return@coroutineScope search.aggregations()["total"]?.sum()?.value() ?: 0.0
    }

    suspend fun getAllDocsCount(): Long = coroutineScope {
        val search: CountRequest = CountRequest.of {
            it.index(elasticIndex)
            it.query { query ->
                query.bool { bool ->
                    bool.must { must ->
                        must.exists { exists ->
                            exists.field("address.url")
                        }
                    }
                }
            }
        }
        return@coroutineScope client.count(search).count()
    }
}
