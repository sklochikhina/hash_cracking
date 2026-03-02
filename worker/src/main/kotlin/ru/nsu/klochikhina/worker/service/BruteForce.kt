package ru.nsu.klochikhina.worker.service

import constants.Constants
import dto.TaskDto
import org.springframework.stereotype.Component
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

@Component
class BruteForce {

    private val alphabet = Constants.ALPHABET.toCharArray()
    private val base = Constants.BASE
    private val hexArray = "0123456789abcdef".toCharArray()
    private val charIndex: IntArray = IntArray(128) { -1 }.also { idx ->
        for (i in alphabet.indices) {
            val c = alphabet[i]
            if (c.code < idx.size) idx[c.code] = i
        }
    }

    fun findFirstMatch(task: TaskDto): List<String> {
        val startIndex = task.startIndex
        val count = task.count
        val maxLength = task.maxLength
        val targetHash = task.targetHash

        if (count <= 0L) return emptyList()

        val pows = LongArray(maxLength + 1)
        pows[0] = 1L
        for (i in 1..maxLength) pows[i] = pows[i - 1] * base

        fun findLenAndPos(index: Long): Pair<Int, Long> {
            var rem = index
            for (len in 1..maxLength) {
                val block = pows[len]
                if (rem < block) return len to rem
                rem -= block
            }
            throw IllegalArgumentException("index $index out of range for maxLength=$maxLength")
        }

        fun posToCharArray(pos: Long, len: Int): CharArray {
            val arr = CharArray(len)
            var v = pos
            for (posIdx in (len - 1) downTo 0) {
                val d = (v % base).toInt()
                arr[posIdx] = alphabet[d]
                v /= base
            }
            return arr
        }

        fun increment(arr: CharArray): Boolean {
            var i = arr.size - 1
            while (i >= 0) {
                val code = arr[i].code
                val idx = if (code < charIndex.size) charIndex[code] else -1
                if (idx < 0) throw IllegalStateException("Unknown char in arr: ${arr[i]}")
                val next = idx + 1
                if (next < alphabet.size) {
                    arr[i] = alphabet[next]
                    return true
                } else {
                    arr[i] = alphabet[0]
                    i--
                }
            }
            return false
        }

        fun bytesToHex(bytes: ByteArray): String {
            val chars = CharArray(bytes.size * 2)
            var j = 0
            for (b in bytes) {
                val v = b.toInt() and 0xFF
                chars[j++] = hexArray[v ushr 4]
                chars[j++] = hexArray[v and 0x0F]
            }
            return String(chars)
        }

        val md = MessageDigest.getInstance("MD5")
        var idx = startIndex
        val end = startIndex + count
        if (idx < 0L) return emptyList()

        var (len, posInBlock) = findLenAndPos(idx)
        var chars = posToCharArray(posInBlock, len)

        while (idx < end) {
            val candidate = String(chars)
            md.reset()
            val digest = md.digest(candidate.toByteArray(StandardCharsets.UTF_8))
            val hex = bytesToHex(digest)
            if (hex.equals(targetHash, ignoreCase = true)) {
                return listOf(candidate)
            }

            idx++
            val ok = increment(chars)
            if (!ok) {
                val nextLen = len + 1
                if (nextLen > maxLength) break
                len = nextLen
                chars = CharArray(len) { alphabet[0] }
            }
        }

        return emptyList()
    }
}