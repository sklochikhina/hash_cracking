package ru.nsu.klochikhina.manager.model.dto

import jakarta.validation.Validation
import kotlin.test.Test
import kotlin.test.assertEquals

class CreateRequestDtoValidationTest {

    private val validator = Validation.buildDefaultValidatorFactory().validator

    @Test
    fun `valid dto passes validation`() {
        val dto = CreateRequestDto(
            hash = "0cc175b9c0f1b6a831c399e269772661",
            maxLength = 4
        )

        val violations = validator.validate(dto)

        assertEquals(0, violations.size)
    }

    @Test
    fun `blank hash fails validation`() {
        val dto = CreateRequestDto(
            hash = "",
            maxLength = 4
        )

        val violations = validator.validate(dto)

        assertEquals(1, violations.size)
    }

    @Test
    fun `non-positive maxLength fails validation`() {
        val dto = CreateRequestDto(
            hash = "0cc175b9c0f1b6a831c399e269772661",
            maxLength = 0
        )

        val violations = validator.validate(dto)

        assertEquals(1, violations.size)
    }
}
