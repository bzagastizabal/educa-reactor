package com.mitocode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@SpringBootApplication
public class SpringReactorDemoApplication implements CommandLineRunner{

	private static final Logger log = LoggerFactory.getLogger(SpringReactorDemoApplication.class);
	private static List<String> platos = new ArrayList<>();
	
	public static void main(String[] args) {
		platos.add("Hamburguesa");
		platos.add("Pizza");
		platos.add("Soda");
		SpringApplication.run(SpringReactorDemoApplication.class, args);
	}
	
	public void crearMono() {
		Mono<Integer> monoNumero = Mono.just(7);
		monoNumero.subscribe(x -> log.info("Número:" + x));
	}
	
	public void crearFlux() {
		Flux<String> fxPlatos = Flux.fromIterable(platos);
		fxPlatos.subscribe(p -> log.info(p));		
	}
	
	public void FluxToMono() {
		Flux<String> fxPlatos = Flux.fromIterable(platos);
		fxPlatos.collectList().subscribe(lista -> log.info(lista.toString()));	
	}
	
	public void m1doOnNext() {
		Flux<String> fxPlatos = Flux.fromIterable(platos);		
		fxPlatos.doOnNext(p -> log.info(p)).subscribe();		
	}
	
	public void m2map() {
		Flux<String> fxPlatos = Flux.fromIterable(platos);
		//Flux<String> fxPlatos2 = fxPlatos.map(p -> "Plato: " + p);
		//fxPlatos2.subscribe(p -> log.info(p));
		fxPlatos.map(p -> "Plato: " + p).subscribe(p -> log.info(p));
	}
	
	public void m3flatMap() {
		Mono.just("Jaime")
			.flatMap(x -> Mono.just(29))
			.subscribe(p-> log.info(p.toString()));
	}
	
	public void m4range() {
		Flux<Integer> fx1 = Flux.range(0, 10);
		
		fx1.map(x -> { 
			return x + 1; 
		}).subscribe(x -> log.info("N: " + x));			
	}
	
	public void m5delayElements() throws InterruptedException{
		Flux.range(0, 10)
			.delayElements(Duration.ofSeconds(2))
			.doOnNext(i -> log.info(i.toString()))
			.subscribe();				

		Thread.sleep(20000);

	}
	
	public void m6zipWith() {
		List<String> clientes = new ArrayList<>();
		clientes.add("Jaime");
		clientes.add("Code");
		
		Flux<String> fxPlatos = Flux.fromIterable(platos);
		Flux<String> fxClientes = Flux.fromIterable(clientes);
		
		fxPlatos
			.zipWith(fxClientes, (p, c) -> String.format("Flux1: %s, Flux2: %s", p, c))
			.subscribe(x -> log.info(x));
	}
	
	public void m7merge() {
		List<String> clientes = new ArrayList<>();

		clientes.add("Jaime");
		clientes.add("Code");
		
		Flux<String> fxPlatos = Flux.fromIterable(platos);
		Flux<String> fxClientes = Flux.fromIterable(clientes);
		
		Flux.merge(fxPlatos, fxClientes).subscribe(x -> log.info(x));
	}
	
	public void m8filter() {
		Flux<String> fxPlatos = Flux.fromIterable(platos);

		fxPlatos.filter(p -> {
			return p.startsWith("H");			
		}).subscribe(x -> log.info(x));
	}
	
	public void m9takeLast() {
		Flux<String> fxPlatos = Flux.fromIterable(platos);

		//hamburguesa
		//pizza
		//soda
		fxPlatos.takeLast(2).subscribe(x -> log.info(x));
	}
	
	public void m10take() {
		Flux<String> fxPlatos = Flux.fromIterable(platos);
		fxPlatos.take(2).subscribe(x -> log.info(x));
	}
	
	public void m11DefaultIfEmpty() {
		platos = new ArrayList<>();
		//Flux.empty();
		//Mono.empty();
		Flux<String> fxPlatos = Flux.fromIterable(platos);
		fxPlatos.defaultIfEmpty("LISTA VACIA").subscribe(x -> log.info(x));
	}
	
	public void m12onErrorReturn() {		
		Flux<String> fxPlatos = Flux.fromIterable(platos);

		fxPlatos
		.doOnNext(p -> {
			throw new ArithmeticException("MAL CALCULO");
		})
		//.onErrorMap(ex -> new ArithmeticException("MAL CALCULO"))
		.onErrorReturn("OCURRIO UN ERROR")
		.subscribe(x -> log.info(x));		
	}
	
	public void m13retry() {
		Flux<String> fxPlatos = Flux.fromIterable(platos);

		fxPlatos
		.doOnNext(p -> {
			log.info("intentando....");
			throw new ArithmeticException("MAL CALCULO");
		})
		.retry(3)
		.onErrorReturn("ERROR!")
		.subscribe(x -> log.info(x));
	}
	
	
	public void m14Thread() throws Exception{
		final Mono<String> mono = Mono.just("hello ");
		
		Thread t = new Thread( () -> mono
				.map(msg -> msg + "thread: ")
				.subscribe(v -> 
					System.out.println(v + Thread.currentThread().getName())
				)
		);		
		
		System.out.println(Thread.currentThread().getName());
		t.start();
		t.join();
	}
	
	public void m15PublishOn() {
		Flux.range(1, 2)
		.publishOn(Schedulers.single())
		.map(x-> {
			log.info("Valor : " + x + " | Thread: " + Thread.currentThread().getName());
			return x;
		})
		.publishOn(Schedulers.boundedElastic())
		.map(x-> {
			log.info("Valor : " + x + " | Thread: " + Thread.currentThread().getName());
			return x;
		})
		.subscribe();
	}
	
	public void m16SubscribeOn() {
		Flux.range(1, 2)
		.map(x-> {
			log.info("Valor : " + x + " | Thread: " + Thread.currentThread().getName());
			return x;
		})		
		.map(x-> {
			log.info("Valor : " + x + " | Thread: " + Thread.currentThread().getName());
			return x;
		})		
		.map(x-> {
			log.info("Valor : " + x + " | Thread: " + Thread.currentThread().getName());
			return x;
		})
		.subscribeOn(Schedulers.boundedElastic())
		.subscribe();
	}
	
	public void m17PublishSubscribeOn() throws Exception{
		///////////
		Flux.range(1, 2)
		//.publishOn(Schedulers.parallel())
		.map(x-> {
			log.info("Valor : " + x + " | Thread: " + Thread.currentThread().getName());
			return x;
		})		
		.subscribeOn(Schedulers.boundedElastic())
		.map(x-> {
			log.info("Valor : " + x + " | Thread: " + Thread.currentThread().getName());
			return x;
		})
		.publishOn(Schedulers.single())
		.map(x-> {
			log.info("Valor : " + x + " | Thread: " + Thread.currentThread().getName());
			return x;
		})
		.subscribe();
		/////////////
		
		Thread.sleep(20000);
	}
	
	
	@Override
	public void run(String... args) throws Exception {
		m17PublishSubscribeOn();
	}

}
