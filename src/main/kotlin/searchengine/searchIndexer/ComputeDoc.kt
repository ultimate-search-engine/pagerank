package searchengine.searchIndexer

import io.ktor.http.*
import libraries.Page
import libraries.PageRepository
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import searchengine.Pagerank
import searchengine.cUrl

class ComputeDoc(
    private val pagerankPage: Pagerank.PagerankPage,
    private val repoDoc: PageRepository.Page,
    private val dbClient: PageRepository.Client
) {
    private val parse = Jsoup.parse(repoDoc.content)
    private val targetUrls = repoDoc.targetUrl.map { Url(it).cUrl() } + Url(repoDoc.finalUrl).cUrl()

    private suspend fun getBacklinkAnchorText() = pagerankPage.backLinks.distinctBy { it.url }.mapNotNull {
        val repoDoc = dbClient.find(it.url).firstOrNull()
        if (repoDoc != null) anchorTextOnDoc(Jsoup.parse(repoDoc.content))
        else null
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
        }.filter { it.isNotEmpty() }.joinToString(" ")
    }


    suspend fun compute(totalDocsCount: Int): Page.Page {
        return Page.Page(
            repoDoc.finalUrl, Page.Ranks(pagerankPage.rank.last(), pagerankPage.rank.last() * totalDocsCount), Page.Content(
                parse.title(),
                parse.select("meta[name=description]").attr("content"),
                listOf(), // possibly more words that may be relevant
                getBacklinkAnchorText(), // inner text of backlinks to this page
                parse.select("b").map { it.text() }.toList(),
                Page.Headings(
                    parse.select("h1").map { it.text() }.toList(),
                    parse.select("h2").map { it.text() }.toList(),
                    parse.select("h3").map { it.text() }.toList(),
                    parse.select("h4").map { it.text() }.toList(),
                    parse.select("h5").map { it.text() }.toList(),
                    parse.select("h6").map { it.text() }.toList()
                ),
                parse.select("p").mapNotNull { if (it.text().count() > 40) it.text() else null }.toList(),
            )
        )
    }


}