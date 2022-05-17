package searchengine.searchIndexer

import io.ktor.http.*
import libraries.Page
import libraries.PageRepository
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import searchengine.pagerank.Pagerank
import searchengine.pagerank.cUrl

class ComputeDoc(
    private val pagerankPage: Pagerank.PagerankPage,
    repoDoc: PageRepository.Page?,
    private val dbClient: PageRepository.Client,
    private val totalDocsCount: Int
) {
    private val parse = if (repoDoc != null) Jsoup.parse(repoDoc.content) else null
    private val targetUrls = pagerankPage.backLinks.map { it.url } + pagerankPage.url

    private suspend fun getBacklinkAnchorText() =
        pagerankPage.backLinks.distinctBy { it.url }.sortedByDescending { it.rank.last() }.take(420).mapNotNull {
            val find = dbClient.find(it.url).firstOrNull() ?: return@mapNotNull null
            if (it.doesForward) {
                ComputeDoc(it, find, dbClient, totalDocsCount).compute().content.anchors.joinToString(" ")
            } else {
                anchorTextOnDoc(Jsoup.parse(find.content))
            }
        }

    private fun anchorTextOnDoc(doc: Document): String {
        val links = doc.select("a")
        return links.mapNotNull {
            try {
                val url = Url(it.attr("href")).cUrl()
                if (url in targetUrls) it.text() else null
            } catch (e: Exception) {
                null
            }
        }.filter { it.count() >= 3 }.joinToString(" ")
    }


    suspend fun compute(): Page.Page {
        return Page.Page(pagerankPage.url,
            Page.Ranks(pagerankPage.rank.last(), pagerankPage.rank.last() * totalDocsCount),
            Page.Content(
                parse?.title() ?: "",
                parse?.select("meta[name=description]")?.attr("content") ?: "",
                listOf(), // possibly more words that may be relevant
                getBacklinkAnchorText(), // inner text of backlinks to this page
                parse?.select("b")?.map { it.text() } ?: listOf(),
                Page.Headings(parse?.select("h1")?.map { it.text() } ?: listOf(),
                    parse?.select("h2")?.map { it.text() } ?: listOf(),
                    parse?.select("h3")?.map { it.text() } ?: listOf(),
                    parse?.select("h4")?.map { it.text() } ?: listOf(),
                    parse?.select("h5")?.map { it.text() } ?: listOf(),
                    parse?.select("h6")?.map { it.text() } ?: listOf()),
                parse?.select("p")?.mapNotNull { if (it.text().count() > 40) it.text() else null } ?: listOf(),
            ))
    }


}