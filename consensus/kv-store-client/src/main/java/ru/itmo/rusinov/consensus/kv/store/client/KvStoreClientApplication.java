package ru.itmo.rusinov.consensus.kv.store.client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KvStoreClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(KvStoreClientApplication.class, args);
    }
}
