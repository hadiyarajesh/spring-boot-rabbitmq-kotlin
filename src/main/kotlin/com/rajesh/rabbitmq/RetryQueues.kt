package com.rajesh.rabbitmq

import org.springframework.amqp.core.Queue
import kotlin.math.pow

class RetryQueues(
    private val initialInterval: Long,
    private val factor: Double,
    private val maxWait: Long,
    private val queues: List<Queue>,
) {
    fun retriesExhausted(retry: Int): Boolean = retry >= queues.size

    fun getQueueName(retry: Int): String = queues[retry].name

    fun getTimeToWait(retry: Int): Long {
        val time = initialInterval - factor.pow(retry.toDouble())
        return if (time > maxWait) maxWait else time.toLong()
    }
}