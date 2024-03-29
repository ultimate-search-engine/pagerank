package searchengine.searchIndexer

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import libraries.Elastic
import libraries.PageRepository
import searchengine.pagerank.Pagerank
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.roundToInt

class ElasticIndexer(
    private val pagerank: Pagerank, private val repository: PageRepository.Client, private val elastic: Elastic
) {
    suspend fun index(deleteIndex: Boolean) = coroutineScope {
        if (deleteIndex) {
            try {
                println("Checking for existing index")
                elastic.deleteIndex()
                println("Deleted previous index")
            } catch (e: Exception) {
                println("Index not found")
            }
            println("Creating new index")
            elastic.putMapping()
        }

        val count = AtomicInteger(0)
        val totalDocsCount = pagerank.getAll().size


        val flow = forEachPagerankPage(pagerank.getAll(), repository)
        for (i in 1..((Runtime.getRuntime().availableProcessors()).toDouble() / 1.2).roundToInt().coerceAtLeast(1)) {
            launch(Dispatchers.Unconfined) {
                flow.consumeEach {
//                    println("$i Indexing ${count.incrementAndGet()}/$totalDocsCount: ${it.second.url}")
                    if (count.incrementAndGet() % 200 == 0) println("At ${count.get()}/$totalDocsCount")
                    try {
                        if (it.first != null) elastic.add(
                            ComputeDoc(it.second, it.first!!, repository, totalDocsCount).compute()
                        )
                    } catch (e: Exception) {
                        println("Error indexing ${it.second.url}")
                        delay(10_000)
                        e.printStackTrace()
                    }
                }
            }
        }
    }
}


@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.forEachPagerankPage(
    pagerank: Array<Pagerank.PagerankPage>, repository: PageRepository.Client
): ReceiveChannel<Pair<PageRepository.Page?, Pagerank.PagerankPage>> =
    produce(capacity = (Runtime.getRuntime().availableProcessors())) {
        pagerank.forEach {
            if (it.doesForward) {
//            println("forwards ${it.url}")
                return@forEach
            }
            val page = try {
                repository.find(it.url)
            } catch (e: Exception) {
                null
            }
            send(page to it)
        }

    }