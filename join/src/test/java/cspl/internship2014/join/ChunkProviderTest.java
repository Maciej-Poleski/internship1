package cspl.internship2014.join;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.BufferedReader;
import java.util.List;
import java.util.Map;

import static cspl.internship2014.join.JoinTest.getInputStreamReaderMaker;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;


public class ChunkProviderTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    InputStreamReaderMaker inputStreamReader1;
    InputStreamReaderMaker inputStreamReader2;
    ChunkProvider chunkProvider1;
    ChunkProvider chunkProvider2;

    @Before
    public void setUp() throws Exception {
        inputStreamReader1 = getInputStreamReaderMaker("test1/in3.txt");
        inputStreamReader2 = getInputStreamReaderMaker("test1/in_broken.txt");
        chunkProvider1 = new ChunkProvider(inputStreamReader1.getInputStreamReader());
        chunkProvider2 = new ChunkProvider(inputStreamReader2.getInputStreamReader());
    }

    @After
    public void tearDown() throws Exception {
        chunkProvider1.close();
        chunkProvider2.close();
    }

    @Test
    public void testGetHeader() throws Exception {
        String[] header = new BufferedReader(inputStreamReader1.getInputStreamReader()).readLine().split(",");
        assertArrayEquals(header, chunkProvider1.getHeader());

        header = new BufferedReader(inputStreamReader2.getInputStreamReader()).readLine().split(",");
        assertArrayEquals(header, chunkProvider2.getHeader());
    }

    @Test
    public void testGetChunk() throws Exception {
        for (; ; ) {
            Map<String, List<String[]>> chunk = chunkProvider1.getChunk(1, 0);
            if (chunk.isEmpty()) {
                break;
            }
        }
    }

    @Test
    public void testGetChunkBrokenInput() throws Exception {
        expectedException.expect(IllegalDataException.class);
        for (; ; ) {
            Map<String, List<String[]>> chunk = chunkProvider2.getChunk(1, 0);
            assertFalse(chunk.isEmpty());
        }
    }
}
