package searchengine

import searchengine.plugins.Server

fun main() = try {
    val server = Server(8083)
    server.run()
} catch (e: Exception) {
    e.printStackTrace()
}