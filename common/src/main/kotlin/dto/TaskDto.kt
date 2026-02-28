package dto

data class TaskDto (
    val taskId: String,
    val requestId: String,
    val startIndex: Long,
    val count: Long,
    val targetHash: String,
    val maxLength: Long
)
