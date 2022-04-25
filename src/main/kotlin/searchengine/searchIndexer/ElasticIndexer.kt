package searchengine.searchIndexer

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import libraries.Elastic
import libraries.PageRepository
import searchengine.pagerank.Pagerank
import java.util.concurrent.atomic.AtomicInteger

class ElasticIndexer(
    private val pagerank: Pagerank,
    private val repository: PageRepository.Client,
    private val elastic: Elastic
) {
    suspend fun index() = coroutineScope {
        try {
            elastic.deleteIndex()
        } catch (e: Exception) {
            println("Index not found")
        }
        elastic.putMapping()

        val count = AtomicInteger(0)
        val totalDocsCount = pagerank.getAll().size


        val flow = forEachPagerankPage(pagerank.getAll(), repository)
        for (i in 1..6) {
            launch(Dispatchers.Unconfined) {
                flow.consumeEach {
//                    println("$i Indexing ${count.incrementAndGet()}/$totalDocsCount: ${it.second.url}")
                    if (count.getAndIncrement() % 200 == 0) println("At ${count.get()}/$totalDocsCount")
                    val page = ComputeDoc(it.second, it.first, repository).compute(totalDocsCount)
                    elastic.add(page)
                }
            }
        }
    }
}


@OptIn(ExperimentalCoroutinesApi::class)
fun CoroutineScope.forEachPagerankPage(
    pagerank: Array<Pagerank.PagerankPage>,
    repository: PageRepository.Client
): ReceiveChannel<Pair<PageRepository.Page?, Pagerank.PagerankPage>> =
    produce(capacity = 4) {
        pagerank.forEach {
            val page = repository.find(it.url).firstOrNull()
            send(page to it)
        }

    }