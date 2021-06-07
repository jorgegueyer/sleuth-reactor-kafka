package es.jgy.back.app;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.sleuth.instrument.kafka.TracingKafkaConsumerFactory;
import org.springframework.cloud.sleuth.instrument.kafka.TracingKafkaProducerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RestController
@SpringBootApplication
public class SleuthReactorKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SleuthReactorKafkaApplication.class, args);
	}

	@GetMapping("/echo/headers")
	public Mono<String> getHeaders(ServerWebExchange serverWebExchange) {
		return Mono.just(serverWebExchange.getRequest().getHeaders().toString());
	}

	@Bean
	KafkaReceiver<Integer, String> reactiveKafkaReceiver(TracingKafkaConsumerFactory tracingKafkaConsumerFactory) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "clientId");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test1-group");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		ReceiverOptions<Integer, String> receiverOptions = ReceiverOptions.create(props);
		ReceiverOptions<Integer, String> options = receiverOptions.subscription(Collections.singleton("test1"));
		return KafkaReceiver.create(tracingKafkaConsumerFactory, options);
	}

	@Bean
	KafkaSender<Integer, String> reactiveKafkaSender(TracingKafkaProducerFactory tracingKafkaProducerFactory) {
		Map<String, Object> props = new HashMap<>();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "producerId");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		SenderOptions<Integer, String> senderOptions = SenderOptions.create(props);
		return KafkaSender.create(tracingKafkaProducerFactory, senderOptions);
	}

	@Autowired
	private WebClient.Builder webClientBuilder;

	@Bean
	public ApplicationRunner runner(KafkaSender<Integer, String> sender,
									KafkaReceiver<Integer, String> receiver) {
		return args -> {
			WebClient webClient = webClientBuilder.baseUrl("http://localhost:8080").build();
			this.sendMessages(sender);
			this.consumeMessages(receiver, webClient);
		};
	}

	private final AtomicInteger atomicInteger = new AtomicInteger();

	public void sendMessages(KafkaSender<Integer, String> sender) {
		sender.send(
			Mono.just("Some event!: ")
				.map(message -> {
					int i = atomicInteger.incrementAndGet();
					return SenderRecord.create(new ProducerRecord<>("test1", i, message + i), i);
				})
		)
		.doOnError(e -> log.error("Send failed", e))
		.subscribe();
	}

	public void consumeMessages(KafkaReceiver<Integer, String> receiver, WebClient webClient) {
		receiver.receive()
			.doOnNext(record ->
				record.headers()
						.forEach(header ->
								log.info("Event message received: {} -> Event Header: {}, value: {}",
										record.value(),
										header.key(),
										new String(header.value(), StandardCharsets.UTF_8)
								)
						)
			)
			.flatMap(record ->
					webClient.get()
							.uri("/echo/headers")
							.retrieve()
							.bodyToMono(String.class)
							.doOnNext(response ->
								log.info("Event message received: {} -> Http headers propagated: {}", record.value(), response)
							)
							.thenReturn(record)
			)
			.subscribe(record -> record.receiverOffset().acknowledge());
	}
}
