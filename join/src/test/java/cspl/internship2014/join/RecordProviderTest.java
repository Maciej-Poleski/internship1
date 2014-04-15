package cspl.internship2014.join;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

import static cspl.internship2014.join.JoinTest.getInputStreamReaderMaker;
import static org.junit.Assert.*;


public class RecordProviderTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    InputStreamReaderMaker inputStreamReader1;
    InputStreamReaderMaker inputStreamReader2;
    RecordProvider recordProvider1;
    RecordProvider recordProvider2;

    @Before
    public void setUp() throws Exception {
        inputStreamReader1 = getInputStreamReaderMaker("test1/in3.txt");
        inputStreamReader2 = getInputStreamReaderMaker("test1/in_broken.txt");
        recordProvider1 = new RecordProvider(inputStreamReader1);
        recordProvider2 = new RecordProvider(inputStreamReader2);
    }

    @After
    public void tearDown() throws Exception {
        recordProvider1.close();
        recordProvider2.close();
    }

    @Test
    public void testGetHeader() throws Exception {
        String[] header = new BufferedReader(inputStreamReader1.getInputStreamReader()).readLine().split(",");
        assertArrayEquals(header, recordProvider1.getHeader());

        header = new BufferedReader(inputStreamReader2.getInputStreamReader()).readLine().split(",");
        assertArrayEquals(header, recordProvider2.getHeader());
    }

    @Test
    public void testReadNextRecord() throws Exception {
        List<String> rows = new ArrayList<String>();
        for (; ; ) {
            String[] r = recordProvider1.readNextRecord();
            if (r == null) {
                break;
            }
            rows.add(StringUtils.join(r, ','));
        }
        BufferedReader reader = new BufferedReader(inputStreamReader1.getInputStreamReader());
        reader.readLine();
        for (String r : rows) {
            assertEquals(r, reader.readLine().replace("\"", ""));
        }
        for (; ; ) {
            String r = reader.readLine();
            if (r == null) {
                break;
            }
            assertTrue(r.trim().isEmpty());
        }
    }

    @Test
    public void testBrokenInput() throws Exception {
        expectedException.expect(IllegalDataException.class);
        for (; ; ) {
            recordProvider2.readNextRecord();
        }
    }

    @Test
    public void testReset() throws Exception {
        List<String[]> rows = new ArrayList<String[]>();
        for (; ; ) {
            String[] r = recordProvider1.readNextRecord();
            if (r == null) {
                break;
            }
            rows.add(r);
        }
        recordProvider1.reset();
        for (String[] r : rows) {
            assertArrayEquals(r, recordProvider1.readNextRecord());
        }
        assertNull(recordProvider1.readNextRecord());
    }
}
