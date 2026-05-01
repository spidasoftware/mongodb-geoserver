package com.spidasoftware.mongodb.cache

import java.lang.ref.SoftReference
import java.lang.reflect.Field
import java.util.UUID

import spock.lang.Specification

class QueryResultCacheSpec extends Specification {

    void "evicts least recently used entry when cache is full"() {
        setup:
            def configured = newConfiguredCache(enableCache: 'true', cacheSize: '2', cacheTTL: '60000')
            def cache = configured.cache

        when:
            cache.put('a', 'value-a')
            cache.put('b', 'value-b')
            cache.get('a')
            cache.put('c', 'value-c')

        then:
            cache.get('a') == 'value-a'
            cache.get('b') == null
            cache.get('c') == 'value-c'
            cache.getStats().size == 2
    }

    void "expired entries are removed on access"() {
        setup:
            def configured = newConfiguredCache(enableCache: 'true', cacheSize: '10', cacheTTL: '5')
            def cache = configured.cache
            cache.put('ttl-key', 'ttl-value')
            def entry = getCacheEntries(cache).get('ttl-key')
            entry.timestamp = System.currentTimeMillis() - 100

        when:
            def value = cache.get('ttl-key')

        then:
            value == null
            !getCacheEntries(cache).containsKey('ttl-key')
            !getAccessOrder(cache).contains('ttl-key')
    }

    void "cleared soft references are purged on access"() {
        setup:
            def configured = newConfiguredCache(enableCache: 'true', cacheSize: '10', cacheTTL: '60000')
            def cache = configured.cache
            cache.put('soft-key', 'soft-value')
            def entry = getCacheEntries(cache).get('soft-key')
            entry.value = new SoftReference<>(null)

        when:
            def value = cache.get('soft-key')

        then:
            value == null
            !getCacheEntries(cache).containsKey('soft-key')
            !getAccessOrder(cache).contains('soft-key')
    }

    void "non positive cache size behaves as disabled"() {
        setup:
            def configured = newConfiguredCache(enableCache: 'true', cacheSize: '0', cacheTTL: '60000')
            Class cacheClass = configured.cacheClass
            def cache = configured.cache

        when:
            cache.put('disabled-key', 'disabled-value')

        then:
            cache.get('disabled-key') == null
            cache.getStats().size == 0
            cache.getStats().maxSize == 0
            !cacheClass.getMethod('isEnabled').invoke(null)
        and:
            getCacheEntries(cache).isEmpty()
            getAccessOrder(cache).isEmpty()
    }

    private Object newConfiguredCache(Map<String, String> properties) {
        return withConfiguredCache(properties) { Class cacheClass ->
            [cacheClass: cacheClass, cache: cacheClass.getDeclaredConstructor().newInstance()]
        }
    }

    private Class loadCacheClass(Map<String, String> properties) {
        return withConfiguredCache(properties) { Class cacheClass ->
            cacheClass
        }
    }

    private <T> T withConfiguredCache(Map<String, String> properties, Closure<T> action) {
        Map<String, String> previousValues = [:]
        properties.each { key, value ->
            String propertyName = "mongodb.geoserver.${key}"
            previousValues[propertyName] = System.getProperty(propertyName)
            if (value == null) {
                System.clearProperty(propertyName)
            } else {
                System.setProperty(propertyName, value)
            }
        }

        GroovyClassLoader classLoader = new GroovyClassLoader(this.class.classLoader)
        try {
            File sourceFile = resolveSourceFile('src/main/groovy/com/spidasoftware/mongodb/cache/QueryResultCache.groovy')
            String uniqueClassName = "QueryResultCacheSpec_${UUID.randomUUID().toString().replace('-', '_')}"
            String source = sourceFile.getText('UTF-8').replaceFirst(/class\s+QueryResultCache\b/, "class ${uniqueClassName}")
            Class cacheClass = classLoader.parseClass(source, "${uniqueClassName}.groovy")
            return action.call(cacheClass)
        } finally {
            previousValues.each { propertyName, previousValue ->
                if (previousValue == null) {
                    System.clearProperty(propertyName)
                } else {
                    System.setProperty(propertyName, previousValue)
                }
            }
            classLoader.close()
        }
    }

    private File resolveSourceFile(String relativePath) {
        File current = new File(System.getProperty('user.dir'))
        while (current != null) {
            File marker = new File(current, 'build.gradle')
            if (marker.exists()) {
                File candidate = new File(current, relativePath)
                if (candidate.exists()) {
                    return candidate
                }
                break
            }
            current = current.parentFile
        }

        File fallback = new File(relativePath)
        if (fallback.exists()) {
            return fallback
        }

        throw new FileNotFoundException("Unable to locate ${relativePath}; user.dir=${System.getProperty('user.dir')}")
    }

    private Map getCacheEntries(Object cache) {
        return (Map) getField(cache, 'cache')
    }

    private LinkedList getAccessOrder(Object cache) {
        return (LinkedList) getField(cache, 'accessOrder')
    }

    private Object getField(Object target, String fieldName) {
        Field field = target.getClass().getDeclaredField(fieldName)
        field.accessible = true
        return field.get(target)
    }
}