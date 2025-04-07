/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelineTests extends OpenSearchTestCase {

    public void testCreatePipelineWithEmptyConfig() {
        final Pipeline pipeline = Pipeline.createIndexBasedIngestPipeline("test-index", Collections.emptyMap(), Collections.emptyMap());

        assertNotNull(pipeline);
        assertEquals("test-index_index_based_ingest_pipeline", pipeline.getId());
        assertTrue(pipeline.getProcessors().isEmpty());
    }

    public void testCreatePipelineWithOneProcessor() throws Exception {
        final Processor processor = mock(Processor.class);
        final Processor.Factory factory = mock(Processor.Factory.class);
        final Map<String, Object> config = Map.of("key", "value");
        when(factory.create(any(), any(), any(), any())).thenReturn(processor);

        final Pipeline pipeline = Pipeline.createIndexBasedIngestPipeline("my-index", Map.of("factory", factory), config);

        assertNotNull(pipeline);
        assertEquals("my-index_index_based_ingest_pipeline", pipeline.getId());
        assertEquals(1, pipeline.getProcessors().size());
        assertSame(processor, pipeline.getProcessors().get(0));

        verify(factory, times(1)).create(any(), any(), any(), any());
    }

    public void testCreatePipelineWithFactoryException() throws Exception {
        final Map<String, Object> config = Map.of("key", "value");
        final Processor.Factory faultyFactory = mock(Processor.Factory.class);
        when(faultyFactory.create(any(), any(), any(), any())).thenThrow(new RuntimeException("Factory failed"));

        final RuntimeException e = assertThrows(
            RuntimeException.class,
            () -> Pipeline.createIndexBasedIngestPipeline("my-index", Map.of("factory", faultyFactory), config)
        );
        assertTrue(e.getMessage().contains("Factory failed"));
    }

    public void testMergeWithNullReturnsOriginal() {
        final Processor processor = mock(Processor.class);
        final Pipeline original = new Pipeline("id", "desc", 1, new CompoundProcessor(processor));

        final Pipeline merged = original.merge(null);

        assertSame(original, merged);
    }

    public void testMergeCombinesProcessorsAndOnFailure() {
        final Processor p1 = mock(Processor.class);
        final Processor p2 = mock(Processor.class);
        final Processor f1 = mock(Processor.class);
        final Processor f2 = mock(Processor.class);

        final CompoundProcessor cp1 = new CompoundProcessor(false, Arrays.asList(p1), Arrays.asList(f1));
        final CompoundProcessor cp2 = new CompoundProcessor(false, Arrays.asList(p2), Arrays.asList(f2));

        final Pipeline pipeline1 = new Pipeline("id", "desc", 1, cp1);
        final Pipeline pipeline2 = new Pipeline("id", "desc", 1, cp2);

        final Pipeline merged = pipeline1.merge(pipeline2);

        List<Processor> mergedProcessors = merged.getProcessors();
        List<Processor> mergedOnFailureProcessors = merged.getOnFailureProcessors();

        assertEquals(2, mergedProcessors.size());
        assertTrue(mergedProcessors.containsAll(Arrays.asList(p1, p2)));

        assertEquals(2, mergedOnFailureProcessors.size());
        assertTrue(mergedOnFailureProcessors.containsAll(Arrays.asList(f1, f2)));
    }

    public void testMergeCreatesNewInstance() {
        final Processor processor = mock(Processor.class);
        final Pipeline pipeline = new Pipeline("id", "desc", 1, new CompoundProcessor(processor));

        final Pipeline merged = pipeline.merge(pipeline);

        assertNotSame(pipeline, merged);
        assertNotSame(pipeline.getProcessors(), merged.getProcessors());
    }
}
