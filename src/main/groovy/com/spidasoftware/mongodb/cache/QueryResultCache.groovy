package com.spidasoftware.mongodb.cache

import org.geotools.data.Query
import org.geotools.util.logging.Logging

import java.lang.ref.SoftReference
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Simple LRU cache for query results using soft references for memory-safe caching.
 * The cache automatically evicts entries when memory pressure is high.
 *
 * Enable caching with -Dmongodb.geoserver.enableCache=true
 * Set cache size with -Dmongodb.geoserver.cacheSize=100
 * Set cache TTL (milliseconds) with -Dmongodb.geoserver.cacheTTL=60000
 */
class QueryResultCache {

    private static final Logger log = Logging.getLogger(QueryResultCache.class.getPackage().getName())

    private static final boolean CACHE_ENABLED = Boolean.getBoolean("mongodb.geoserver.enableCache")
    private static final int MAX_CACHE_SIZE = Integer.getInteger("mongodb.geoserver.cacheSize", 100)
    private static final long CACHE_TTL_MS = Long.getLong("mongodb.geoserver.cacheTTL", 60000) // 1 minute default (milliseconds)

    // Thread-safe cache using soft references
    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>()

    // Track access order for LRU eviction
    private final LinkedList<String> accessOrder = new LinkedList<>()

    static class CacheEntry {
        SoftReference<Object> value
        long timestamp

        CacheEntry(Object value) {
            this.value = new SoftReference<>(value)
            this.timestamp = System.currentTimeMillis()
        }

        boolean isExpired() {
            return System.currentTimeMillis() - timestamp > CACHE_TTL_MS
        }

        Object getValue() {
            return value.get()
        }
    }

    /**
     * Generate a cache key from a Query object.
     */
    String generateKey(String collectionName, Query query) {
        if (query == null) {
            return "${collectionName}:ALL"
        }

        StringBuilder key = new StringBuilder()
        key.append(collectionName).append(":")
        key.append(query.getTypeName() ?: "").append(":")
        key.append(query.getFilter()?.toString() ?: "INCLUDE").append(":")
        key.append(query.getMaxFeatures() ?: "").append(":")
        key.append(query.getStartIndex() ?: "0")

        if (query.getPropertyNames() != null) {
            key.append(":").append(query.getPropertyNames().sort().join(","))
        }

        return key.toString()
    }

    /**
     * Get a cached result if available and valid.
     */
    def get(String key) {
        if (!CACHE_ENABLED) {
            return null
        }

        CacheEntry entry = cache.get(key)
        if (entry == null) {
            return null
        }

        if (entry.isExpired()) {
            cache.remove(key)
            synchronized (accessOrder) {
                accessOrder.remove(key)
            }
            return null
        }

        Object value = entry.getValue()
        if (value == null) {
            // Soft reference was cleared
            cache.remove(key)
            synchronized (accessOrder) {
                accessOrder.remove(key)
            }
            return null
        }

        // Update access order
        synchronized (accessOrder) {
            accessOrder.remove(key)
            accessOrder.addFirst(key)
        }

        return value
    }

    /**
     * Store a result in the cache.
     */
    void put(String key, Object value) {
        if (!CACHE_ENABLED || value == null) {
            return
        }

        // Evict old entries if needed
        while (cache.size() >= MAX_CACHE_SIZE) {
            evictOldest()
        }

        cache.put(key, new CacheEntry(value))
        synchronized (accessOrder) {
            accessOrder.remove(key)
            accessOrder.addFirst(key)
        }
    }

    /**
     * Evict the least recently used entry.
     */
    private void evictOldest() {
        String oldestKey = null
        synchronized (accessOrder) {
            if (!accessOrder.isEmpty()) {
                oldestKey = accessOrder.removeLast()
            }
        }
        if (oldestKey != null) {
            cache.remove(oldestKey)
        }
    }

    /**
     * Clear all cached entries.
     */
    void clear() {
        cache.clear()
        synchronized (accessOrder) {
            accessOrder.clear()
        }
    }

    /**
     * Invalidate cache entries for a specific collection.
     */
    void invalidateCollection(String collectionName) {
        List<String> keysToRemove = cache.keySet().findAll { it.startsWith(collectionName + ":") }
        keysToRemove.each { key ->
            cache.remove(key)
            synchronized (accessOrder) {
                accessOrder.remove(key)
            }
        }
    }

    /**
     * Check if caching is enabled.
     */
    static boolean isEnabled() {
        return CACHE_ENABLED
    }

    /**
     * Get cache statistics.
     */
    Map<String, Object> getStats() {
        return [
            enabled: CACHE_ENABLED,
            size: cache.size(),
            maxSize: MAX_CACHE_SIZE,
            ttlMs: CACHE_TTL_MS
        ]
    }
}
