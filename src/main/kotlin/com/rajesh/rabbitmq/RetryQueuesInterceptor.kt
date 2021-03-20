package com.rajesh.rabbitmq

import org.aopalliance.intercept.MethodInterceptor
import org.aopalliance.intercept.MethodInvocation
import org.springframework.amqp.core.Message
import org.springframework.amqp.rabbit.core.RabbitTemplate
import java.util.function.BiConsumer
import java.util.function.Consumer
import com.rabbitmq.client.Channel

class RetryQueuesInterceptor(
    private val rabbitTemplate: RabbitTemplate,
    private val retryQueues: RetryQueues,
) : MethodInterceptor {
    private lateinit var observer: Runnable

    override fun invoke(invocation: MethodInvocation): Any? {
        return tryConsume(invocation, ::ack) { mac, e ->
            try {
                val retryCount = tryGetRetryCountOrFail(mac, e)
                sendToNextRetryQueue(mac, retryCount)
            } catch (t: Throwable) {
                observer.run()
                throw RuntimeException(t)
            }
        }
    }

    fun setObserver(observer: Runnable) {
        this.observer = observer
    }

    private fun tryConsume(
        invocation: MethodInvocation,
        successHandler: Consumer<MessageAndChannel>,
        errorHandler: BiConsumer<MessageAndChannel, Throwable>
    ): Any? {
        val mac = MessageAndChannel(invocation.arguments[1] as Message, invocation.arguments[0] as Channel)
        var rat: Any? = null
        try {
            rat = invocation.proceed()
            successHandler.accept(mac)
        } catch (e: Throwable) {
            errorHandler.accept(mac, e)
        }
        return rat
    }

    private fun ack(mac: MessageAndChannel) {
        try {
            mac.channel.basicAck(mac.message.messageProperties.deliveryTag, false)
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    private fun tryGetRetryCountOrFail(mac: MessageAndChannel, originalError: Throwable): Int {
        val props = mac.message.messageProperties
        val xRetriedCountHeader: String? = props.getHeader<String?>("x-retried-count")
        val xRetriedCount = xRetriedCountHeader?.toInt() ?: 0
        if (retryQueues.retriesExhausted(xRetriedCount)) {
            mac.channel.basicReject(props.deliveryTag, false)
            throw originalError
        }
        return xRetriedCount
    }

    @Throws(java.lang.Exception::class)
    private fun sendToNextRetryQueue(mac: MessageAndChannel, retryCount: Int) {
        val retryQueueName = retryQueues.getQueueName(retryCount)
        rabbitTemplate.convertAndSend(retryQueueName, mac.message) { m: Message ->
            val props = m.messageProperties
            props.expiration = retryQueues.getTimeToWait(retryCount).toString()
            props.setHeader("x-retried-count", (retryCount + 1).toString())
            props.setHeader("x-original-exchange", props.receivedExchange)
            props.setHeader("x-original-routing-key", props.receivedRoutingKey)
            m
        }

        mac.channel.basicReject(mac.message.messageProperties.deliveryTag, false)
    }

    data class MessageAndChannel(
        val message: Message,
        val channel: Channel
    )
}