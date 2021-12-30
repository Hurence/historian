package com.hurence.historian.greensights.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Configuration
public class CacheConfig {




    @Bean
    public CacheManager cacheManager() {
        CaffeineCache helloCache = new CaffeineCache("pagesize",
                Caffeine.newBuilder().expireAfterAccess(12, TimeUnit.HOURS).build());
        SimpleCacheManager manager = new SimpleCacheManager();
        manager.setCaches(Collections.singletonList(helloCache));
        return manager;
    }
}
