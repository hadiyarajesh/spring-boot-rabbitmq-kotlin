package com.rajesh.rabbitmq

import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import java.lang.Exception
import java.util.concurrent.CountDownLatch
import org.springframework.beans.factory.annotation.Autowired


@RunWith(SpringJUnit4ClassRunner::class)
@ContextConfiguration(classes = [RabbitConfiguration::class])
@SpringBootTest
class RabbitmqApplicationTests @Autowired constructor(
    private val rabbitTemplate: RabbitTemplate,
    private val retryQueues: RetryQueuesInterceptor
) {

    @Test
    fun contextLoads() {
    }

    @Test
    @Throws(Exception::class)
    fun whenSendToNonBlockingQueue_thenAllMessageProcessed() {
        val numberOfMessage = 2
        val latch = CountDownLatch(numberOfMessage)
        retryQueues.setObserver { latch.countDown() }

        (1..numberOfMessage).forEach {
            rabbitTemplate.convertAndSend("non-blocking-queue", "non-blocking message $it")
        }

        latch.await()
    }
}
