package dto

import enums.ResultStatus

data class ResultDto(
    val taskId: String,
    val requestId: String,
    val results: List<String>,
    val status: ResultStatus
)