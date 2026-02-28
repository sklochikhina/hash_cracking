package dto

import enums.ResultStatus

data class ResultDto(
    val taskId: String,
    val requestId: String,
    val result: String,
    val status: ResultStatus
)