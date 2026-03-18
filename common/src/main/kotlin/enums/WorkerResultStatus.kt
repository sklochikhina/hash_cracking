package enums

enum class WorkerResultStatus {
    DONE,   /* задача успешно обработана воркером (но результата может не быть) */
    ERROR   /* ошибка обработки */
}
