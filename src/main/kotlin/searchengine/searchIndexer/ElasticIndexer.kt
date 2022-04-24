package searchengine.searchIndexer

import io.ktor.http.*
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.coroutineScope
import libraries.*
import searchengine.Pagerank
import searchengine.cUrl
import searchengine.repositoryDocs
import java.util.concurrent.atomic.AtomicInteger

class ElasticIndexer(
    private val pagerank: Pagerank,
    private val repository: PageRepository.Client,
    private val elastic: Elastic
) {
    suspend fun index() = coroutineScope {
//        pagerank.getAll().forEachIndexed { i, it ->
//            println("At $i")
//            if (!it.doesForward) {
        // ...
//            } else null
//        }
        try {
            elastic.deleteIndex()
        } catch (e: Exception) {
            println("Index not found")
        }
        elastic.putMapping()

        var i = AtomicInteger(0)
        val totalDocsCount = pagerank.getAll().size

        pagerank.getAll().sortedByDescending { it.rank.last() }.chunked(6).forEach { chunk ->
            chunk.forEach {
                if (!it.doesForward) {
                    val repoPage = repository.find(it.url).firstOrNull()
                    if (repoPage != null) {
                        println("Indexing ${i.incrementAndGet()}/$totalDocsCount: ${repoPage.finalUrl}")
                        val xd = ComputeDoc(it, repoPage, repository).compute(totalDocsCount)
                        elastic.add(xd)
                    }
                }
            }
        }

    }


}