package ru.nsu.klochikhina.manager.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.concurrent.Executor

@Configuration
@EnableAsync
class AsyncConfig {

    @Bean("taskExecutor")
    fun taskExecutor(): Executor {
        val executor = ThreadPoolTaskExecutor()
        executor.corePoolSize = 4      // сколько потоков держится постоянно
        executor.maxPoolSize = 8       // максимум при пиковых нагрузках
        executor.queueCapacity = 100 // очередь задач перед созданием новых потоков
        executor.setThreadNamePrefix("task-splitter-")
        executor.initialize()
        return executor
    }
}