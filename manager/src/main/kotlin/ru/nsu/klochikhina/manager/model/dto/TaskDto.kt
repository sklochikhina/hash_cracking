package ru.nsu.klochikhina.manager.model.dto

data class TaskDto(
    val taskId: String,
    val requestId: String,
    val startIndex: Int,
    val count: Int,
    val targetHash: String,
    val maxLength: Int
)
