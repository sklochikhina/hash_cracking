package ru.nsu.klochikhina.worker.service

import dto.TaskDto
import kotlin.test.Test
import kotlin.test.assertEquals

class BruteForceTest {

    private val bruteForce = BruteForce()

    @Test
    fun `finds match in one-symbol range`() {
        val task = TaskDto(
            taskId = "task-1",
            requestId = "request-1",
            startIndex = 0,
            count = 36,
            targetHash = "0cc175b9c0f1b6a831c399e269772661", // md5("a")
            maxLength = 1
        )

        val result = bruteForce.findFirstMatch(task)

        assertEquals(listOf("a"), result)
    }

    @Test
    fun `finds match when range starts in the middle`() {
        val task = TaskDto(
            taskId = "task-2",
            requestId = "request-2",
            startIndex = 36,
            count = 20,
            targetHash = "4124bc0a9335c27f086f24ba207a4912", // md5("aa")
            maxLength = 2
        )

        val result = bruteForce.findFirstMatch(task)

        assertEquals(listOf("aa"), result)
    }

    @Test
    fun `returns empty result when hash is not present in task range`() {
        val task = TaskDto(
            taskId = "task-3",
            requestId = "request-3",
            startIndex = 0,
            count = 10,
            targetHash = "900150983cd24fb0d6963f7d28e17f72", // md5("abc")
            maxLength = 1
        )

        val result = bruteForce.findFirstMatch(task)

        assertEquals(emptyList(), result)
    }
}
