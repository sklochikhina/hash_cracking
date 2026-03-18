package enums

enum class TaskStatus {
    QUEUED,     /* статус таски по умолчанию при добавлении в БД */
    SENT,       /* таска отправлена в Rabbit */
    SENDING,    /* попытка отправить таску в Rabbit */
    DONE,       /* таска обработана */
    ERROR       /* ошибка обработки, нужна ли? */
}
