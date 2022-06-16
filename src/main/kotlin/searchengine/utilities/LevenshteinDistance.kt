package searchengine.utilities

fun levenshteinDistance(lhs: CharSequence, rhs: CharSequence): Int {
    val lhsLength = lhs.length + 1
    val rhsLength = rhs.length + 1

    var cost = Array(lhsLength) { it }
    var newCost = Array(lhsLength) { 0 }

    for (i in 1 until rhsLength) {
        newCost[0] = i

        for (j in 1 until lhsLength) {
            val match = if (lhs[j - 1] == rhs[i - 1]) 0 else 1

            val costReplace = cost[j - 1] + match
            val costInsert = cost[j] + 1
            val costDelete = newCost[j - 1] + 1

            newCost[j] = costInsert.coerceAtMost(costDelete).coerceAtMost(costReplace)
        }

        val swap = cost
        cost = newCost
        newCost = swap
    }

    return cost[lhsLength - 1]
}

fun levenshteinSimilarity(lhs: CharSequence, rhs: CharSequence): Double = 1.0 - levenshteinDistance(lhs, rhs).toDouble() / lhs.length


fun meldSimilar(list: List<String>, similarity: Double = .90): List<String> {
    val result = mutableListOf<String>()
    val map = mutableMapOf<String, String>()

    for (item in list) {
        val similar = map.filter { levenshteinSimilarity(it.value, item) > similarity }.keys.firstOrNull()
        if (similar == null) {
            map[item] = item
            result.add(item)
        } else {
            map[item] = similar
        }
    }

    return result
}
