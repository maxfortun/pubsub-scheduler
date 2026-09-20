package net.maxf.pubsub.scheduler.rest;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PagedResultTest {

    @Test
    void of_withMoreResults_hasMoreTrue() {
        var result = JobResource.PagedResult.of(List.of("a", "b"), 0, 10, 100);

        assertEquals(List.of("a", "b"), result.items());
        assertEquals(0, result.offset());
        assertEquals(10, result.limit());
        assertEquals(100, result.total());
        assertTrue(result.hasMore());
    }

    @Test
    void of_atExactEnd_hasMoreFalse() {
        var result = JobResource.PagedResult.of(List.of("a", "b"), 8, 2, 10);

        assertEquals(8, result.offset());
        assertEquals(2, result.limit());
        assertEquals(10, result.total());
        assertFalse(result.hasMore());
    }

    @Test
    void of_emptyResults_hasMoreFalse() {
        var result = JobResource.PagedResult.of(List.of(), 0, 100, 0);

        assertTrue(result.items().isEmpty());
        assertEquals(0, result.total());
        assertFalse(result.hasMore());
    }

    @Test
    void of_partialLastPage_hasMoreFalse() {
        var result = JobResource.PagedResult.of(List.of("a"), 9, 10, 10);

        assertEquals(1, result.items().size());
        assertEquals(10, result.total());
        assertFalse(result.hasMore());
    }

    @Test
    void of_firstPageOfMany_hasMoreTrue() {
        var result = JobResource.PagedResult.of(List.of("a", "b", "c"), 0, 3, 100);

        assertEquals(3, result.items().size());
        assertEquals(0, result.offset());
        assertTrue(result.hasMore());
    }

    @Test
    void of_middlePage_hasMoreTrue() {
        var result = JobResource.PagedResult.of(List.of("x", "y"), 50, 10, 100);

        assertEquals(2, result.items().size());
        assertEquals(50, result.offset());
        assertTrue(result.hasMore());
    }

    @Test
    void of_singleItemTotal_hasMoreFalse() {
        var result = JobResource.PagedResult.of(List.of("only"), 0, 100, 1);

        assertEquals(1, result.items().size());
        assertEquals(1, result.total());
        assertFalse(result.hasMore());
    }

    @Test
    void of_offsetBeyondTotal_hasMoreFalse() {
        var result = JobResource.PagedResult.of(List.of(), 100, 10, 50);

        assertTrue(result.items().isEmpty());
        assertEquals(100, result.offset());
        assertEquals(50, result.total());
        assertFalse(result.hasMore());
    }
}
