package searchengine.searchIndexer

import io.ktor.http.*
import libraries.Page
import libraries.PageRepository
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import searchengine.BacklinkAnchorTextMaxCount
import searchengine.pagerank.Pagerank
import searchengine.pagerank.cUrl
import searchengine.utilities.meldSimilar

class ComputeDoc(
    private val pagerankPage: Pagerank.PagerankPage,
    repoDoc: PageRepository.Page,
    private val dbClient: PageRepository.Client,
    private val totalDocsCount: Int
) {
    private val parse = try {
        Jsoup.parse(repoDoc.content)
    } catch (e: Exception) {
        throw Exception("Error parsing page content: ${repoDoc.finalUrl}")
    }
    private val targetUrls = pagerankPage.backLinks.map { it.url } + pagerankPage.url

    private suspend fun getBacklinkAnchorText(): List<String> {
        var ctr = 0
        return backDocs().sortedByDescending { it.rank.last() }.mapNotNull { pagerankPage ->
            if (ctr >= BacklinkAnchorTextMaxCount) null
            else {
                val dbDoc = try {
                    dbClient.find(pagerankPage.url)
                } catch (e: Exception) {
                    null
                } ?: return@mapNotNull null

                val anchorText = anchorTextOnDoc(Jsoup.parse(dbDoc.content)).filter { it.count() in 3..72 }
                ctr += anchorText.size
                anchorText
            }
        }.flatten()
    }

    private fun backDocs(): List<Pagerank.PagerankPage> = pagerankPage.backLinks.map {
        if (it.doesForward) it.backLinks else mutableListOf(it)
    }.flatten().distinctBy { it.url }

    private fun anchorTextOnDoc(doc: Document): List<String> {
        val links = doc.select("a")
        return meldSimilar(links.mapNotNull {
            try {
                val url = Url(it.attr("href")).cUrl()
                if (url in targetUrls) it.text() else null
            } catch (e: Exception) {
                null
            }
        }, .80)
    }


    suspend fun compute(): Page.Page {
        val url = Url(pagerankPage.url)
        return Page.Page(Page.Address(
            pagerankPage.url,
            meldSimilar(url.pathSegments.filter { it.count() >= 3 }.map { it.split(" ", "-", "_") }.flatten(), .80),
            url.host,
        ),
            Page.Ranks(pagerankPage.rank.last(), pagerankPage.rank.last() * totalDocsCount),
            Page.Content(
                parse?.title() ?: "",
                parse?.select("meta[name=description]")?.attr("content") ?: "",
                listOf(), // possibly more words that may be relevant
                getBacklinkAnchorText(), // inner text of backlinks to this page
                parse?.select("b")?.map { it.text() } ?: listOf(),
                Page.Headings(meldSimilar(parse?.select("h1")?.map { it.text() } ?: listOf(), .80),
                    meldSimilar(parse?.select("h2")?.map { it.text() } ?: listOf(), .80),
                    meldSimilar(parse?.select("h3")?.map { it.text() } ?: listOf(), .80),
                    meldSimilar(parse?.select("h4")?.map { it.text() } ?: listOf(), .80),
                    meldSimilar(parse?.select("h5")?.map { it.text() } ?: listOf(), .80),
                    meldSimilar(parse?.select("h6")?.map { it.text() } ?: listOf(), .80)),
                meldSimilar(parse?.select("p")?.mapNotNull { if (it.text().count() > 40) it.text() else null }
                    ?: listOf(), .80),
            ))
    }


}