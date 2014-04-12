package cspl.internship2014.join;

import au.com.bytecode.opencsv.CSVReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;

public class JoinTest {

    @Test
    public void innerJoin() throws Exception {
        InputStreamReaderMaker inputStreamReader1 = getInputStreamReaderMaker("test1/in1.txt");
        InputStreamReaderMaker inputStreamReader2 = getInputStreamReaderMaker("test1/in2.txt");
        InputStreamReaderMaker inputStreamReaderOut = getInputStreamReaderMaker("test1/out_inner.txt");

        InnerJoin join = new InnerJoin();

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        join.join(inputStreamReader1, inputStreamReader2, "id", new OutputStreamWriter(os), JoinBase.SizeHint.LEFT_SHORT);
        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOut.getInputStreamReader(), new InputStreamReader(is)));

        os.reset();
        join.join(inputStreamReader1, inputStreamReader2, "id", new OutputStreamWriter(os), JoinBase.SizeHint.RIGHT_SHORT);
        is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOut.getInputStreamReader(), new InputStreamReader(is)));
    }

    private InputStreamReaderMaker getInputStreamReaderMaker(final String name) {
        return new InputStreamReaderMaker() {
            @Override
            public InputStreamReader getInputStreamReader() throws FileNotFoundException {
                InputStream resourceAsStream = JoinTest.class.getClassLoader().getResourceAsStream(name);
                if (resourceAsStream == null) {
                    Assert.fail("Resource not found: " + name);
                }
                InputStreamReader inputStreamReader = new InputStreamReader(resourceAsStream, Charset.forName("utf-8"));
                return inputStreamReader;
            }
        };
    }

    private boolean csvStreamsEquals(InputStreamReader left, InputStreamReader right) {
        try {
            CSVReader leftReader = new CSVReader(left);
            CSVReader rightReader = new CSVReader(right);
            for (; ; ) {
                String[] leftRow = leftReader.readNext();
                String[] rightRow = rightReader.readNext();
                if (!Arrays.equals(leftRow, rightRow)) {
                    return false;
                }
                if (leftRow == null && rightRow == null) {
                    return true;
                }
            }
        } catch (IOException e) {
            return false;
        }
    }
}
