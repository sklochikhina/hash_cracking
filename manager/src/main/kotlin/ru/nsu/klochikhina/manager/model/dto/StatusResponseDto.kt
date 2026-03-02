package ru.nsu.klochikhina.manager.model.dto

import enums.RequestStatus

data class StatusResponseDto(
    val requestId: String,
    val status: RequestStatus,
    val results: List<String>
)
