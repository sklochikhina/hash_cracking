package enums

enum class RequestStatus {
    PENDING,        /* вспомогательный статус для managers */
    IN_PROGRESS,    /* клиенту возвращается этот статус всегда до готовности ответа,
                        но для программы является маркером конца отправки тасок */
    READY,
    ERROR
}
