package searchengine.plugins

import co.elastic.clients.elasticsearch.core.search.Hit
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import searchengine.elastic.Elastic
import searchengine.pagerank.PagerankCompute
import searchengine.types.PageType

@Serializable
data class ByUrlRequest(val url: String, val computeId: Long)

@Serializable
data class ByNotMatchingComputeRoundIdRequest(val maxCount: Int, val computeId: Long)

@Serializable
data class PredictPagerankResponse(val pagerank: Double)


class Server(port: Int) {
    private val elasticIndex = "se14"
    private val elastic = Elastic(elasticIndex)
    private val pagerankCompute = PagerankCompute(elastic)

    private suspend fun handlePagerankByDoc(doc: Hit<PageType>, computeId: Long) {
        val source = doc.source()
        if (source != null) {
            pagerankCompute.getPagerank(source)
            source.inferredData.ranks.computeRoundId = computeId
            elastic.indexPage(source, doc.id())
        }
    }

    private val server = embeddedServer(Netty, port) {
        install(ContentNegotiation) {
            json()
        }
        routing {
            post("/predictPagerank") {
                val reqBody = call.receive<PageType>()
                val pagerank = withContext(Dispatchers.Default) {
                    pagerankCompute.getPagerank(reqBody)
                }
                println("pagerank: $pagerank")
                call.respond(PredictPagerankResponse(pagerank))
            }

            get("/predictPagerank") {
                call.respond(0.0)
            }

            post("/computePagerankInBulk") {
                val reqBody = call.receive<ByNotMatchingComputeRoundIdRequest>()
                call.respond(200)

                for (i in 0..10) {
                    val docs = elastic.getDocsThatDoesNotMatchComputeRoundId(reqBody.computeId, 200)
                    if (docs.isNotEmpty()) {
                        for (doc in docs) {
                            withContext(Dispatchers.Default) {
                                handlePagerankByDoc(doc, reqBody.computeId)
                            }
                        }
                    }
                }
            }


        }
    }

    fun run() {
        server.start(wait = true)
    }
}
