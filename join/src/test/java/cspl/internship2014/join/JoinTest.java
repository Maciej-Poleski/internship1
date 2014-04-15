package cspl.internship2014.join;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JoinTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void innerJoin() throws Exception {
        InputStreamReaderMaker inputStreamReader1 = getInputStreamReaderMaker("test1/in1.txt");
        InputStreamReaderMaker inputStreamReader2 = getInputStreamReaderMaker("test1/in2.txt");
        InputStreamReaderMaker inputStreamReader3 = getInputStreamReaderMaker("test1/in3.txt");
        InputStreamReaderMaker inputStreamReader4 = getInputStreamReaderMaker("test1/in4.txt");
        InputStreamReaderMaker inputStreamReaderOutInner = getInputStreamReaderMaker("test1/out_inner.txt");
        InputStreamReaderMaker inputStreamReaderOutInner34 = getInputStreamReaderMaker("test1/out_inner34.txt");

        JoinBase join = new InnerJoin();

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        join.join(inputStreamReader1, inputStreamReader2, "id", new OutputStreamWriter(os), JoinBase.SizeHint.LEFT_SHORT);
        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOutInner.getInputStreamReader(), new InputStreamReader(is)));

        os.reset();
        join.join(inputStreamReader1, inputStreamReader2, "id", new OutputStreamWriter(os), JoinBase.SizeHint.RIGHT_SHORT);
        is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOutInner.getInputStreamReader(), new InputStreamReader(is)));

        os.reset();
        join.join(inputStreamReader3, inputStreamReader4, "id", new OutputStreamWriter(os), JoinBase.SizeHint.LEFT_SHORT);
        is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOutInner34.getInputStreamReader(), new InputStreamReader(is)));

        os.reset();
        join.join(inputStreamReader3, inputStreamReader4, "id", new OutputStreamWriter(os), JoinBase.SizeHint.RIGHT_SHORT);
        is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOutInner34.getInputStreamReader(), new InputStreamReader(is)));
    }

    @Test
    public void testBrokenJoinInput1() throws Exception {
        InputStreamReaderMaker inputStreamReader3 = getInputStreamReaderMaker("test1/in3.txt");
        InputStreamReaderMaker inputStreamReader4 = getInputStreamReaderMaker("test1/in_broken.txt");
        JoinBase join = new InnerJoin();

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        expectedException.expect(IllegalDataException.class);
        join.join(inputStreamReader3, inputStreamReader4, "id", new OutputStreamWriter(os), JoinBase.SizeHint.LEFT_SHORT);
    }

    @Test
    public void testBrokenJoinInput2() throws Exception {
        InputStreamReaderMaker inputStreamReader3 = getInputStreamReaderMaker("test1/in3.txt");
        InputStreamReaderMaker inputStreamReader4 = getInputStreamReaderMaker("test1/in_broken.txt");
        JoinBase join = new InnerJoin();

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        expectedException.expect(IllegalDataException.class);
        join.join(inputStreamReader3, inputStreamReader4, "id", new OutputStreamWriter(os), JoinBase.SizeHint.RIGHT_SHORT);
    }

    @Test
    public void testBrokenJoinInputFileNotFound() throws Exception {
        InputStreamReaderMaker inputStreamReader3 = getInputStreamReaderMaker("test1/in3.txt");
        InputStreamReaderMaker inputStreamReader4 = new InputStreamReaderMaker() {
            @Override
            public InputStreamReader getInputStreamReader() throws FileNotFoundException {
                throw new FileNotFoundException();
            }
        };
        JoinBase join = new InnerJoin();

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        expectedException.expect(IllegalApplicationArgumentException.class);
        join.join(inputStreamReader3, inputStreamReader4, "id", new OutputStreamWriter(os), JoinBase.SizeHint.LEFT_SHORT);
    }

    @Test
    public void testBrokenJoinKey() throws Exception {
        InputStreamReaderMaker inputStreamReader3 = getInputStreamReaderMaker("test1/in3.txt");
        InputStreamReaderMaker inputStreamReader4 = getInputStreamReaderMaker("test1/in4.txt");
        JoinBase join = new InnerJoin();

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        expectedException.expect(IllegalDataException.class);
        join.join(inputStreamReader3, inputStreamReader4, "col2", new OutputStreamWriter(os), JoinBase.SizeHint.LEFT_SHORT);
    }

    @Test
    public void testLeftJoin() throws Exception {
        InputStreamReaderMaker inputStreamReader1 = getInputStreamReaderMaker("test1/in1.txt");
        InputStreamReaderMaker inputStreamReader2 = getInputStreamReaderMaker("test1/in2.txt");
        InputStreamReaderMaker inputStreamReader3 = getInputStreamReaderMaker("test1/in3.txt");
        InputStreamReaderMaker inputStreamReader4 = getInputStreamReaderMaker("test1/in4.txt");
        InputStreamReaderMaker inputStreamReaderOutLeft = getInputStreamReaderMaker("test1/out_left.txt");
        InputStreamReaderMaker inputStreamReaderOutLeft34 = getInputStreamReaderMaker("test1/out_left34.txt");

        JoinBase join = new LeftOuterJoin();
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        join.join(inputStreamReader1, inputStreamReader2, "id", new OutputStreamWriter(os), JoinBase.SizeHint.RIGHT_SHORT);
        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOutLeft.getInputStreamReader(), new InputStreamReader(is)));

        os.reset();
        join.join(inputStreamReader3, inputStreamReader4, "id", new OutputStreamWriter(os), JoinBase.SizeHint.LEFT_SHORT);
        is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOutLeft34.getInputStreamReader(), new InputStreamReader(is)));

        os.reset();
        join.join(inputStreamReader3, inputStreamReader4, "id", new OutputStreamWriter(os), JoinBase.SizeHint.RIGHT_SHORT);
        is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOutLeft34.getInputStreamReader(), new InputStreamReader(is)));
    }

    @Test
    public void testRightJoin() throws Exception {
        InputStreamReaderMaker inputStreamReader1 = getInputStreamReaderMaker("test1/in1.txt");
        InputStreamReaderMaker inputStreamReader2 = getInputStreamReaderMaker("test1/in2.txt");
        InputStreamReaderMaker inputStreamReader3 = getInputStreamReaderMaker("test1/in3.txt");
        InputStreamReaderMaker inputStreamReader4 = getInputStreamReaderMaker("test1/in4.txt");
        InputStreamReaderMaker inputStreamReaderOutRight = getInputStreamReaderMaker("test1/out_right.txt");
        InputStreamReaderMaker inputStreamReaderOutRight34 = getInputStreamReaderMaker("test1/out_right34.txt");

        JoinBase join = new RightOuterJoin();
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        join.join(inputStreamReader1, inputStreamReader2, "id", new OutputStreamWriter(os), JoinBase.SizeHint.RIGHT_SHORT);
        ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOutRight.getInputStreamReader(), new InputStreamReader(is)));

        os.reset();
        join.join(inputStreamReader3, inputStreamReader4, "id", new OutputStreamWriter(os), JoinBase.SizeHint.LEFT_SHORT);
        is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOutRight34.getInputStreamReader(), new InputStreamReader(is)));

        os.reset();
        join.join(inputStreamReader3, inputStreamReader4, "id", new OutputStreamWriter(os), JoinBase.SizeHint.RIGHT_SHORT);
        is = new ByteArrayInputStream(os.toByteArray());
        Assert.assertTrue(csvStreamsEquals(inputStreamReaderOutRight34.getInputStreamReader(), new InputStreamReader(is)));
    }

    static InputStreamReaderMaker getInputStreamReaderMaker(final String name) {
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

    private static boolean csvStreamsEquals(InputStreamReader left, InputStreamReader right) {
        CSVReader leftReader = null;
        CSVReader rightReader = null;
        try {
            leftReader = new CSVReader(left);
            rightReader = new CSVReader(right);
            if (!Arrays.equals(leftReader.readNext(), rightReader.readNext())) {
                return false;
            }
            List<String> leftSide = new ArrayList<String>();
            List<String> rightSide = new ArrayList<String>();
            for (; ; ) {
                String[] leftRow = leftReader.readNext();
                String[] rightRow = rightReader.readNext();
                if (leftRow == null && rightRow == null) {
                    break;
                } else if (leftRow == null || rightRow == null) {
                    return false;
                } else {
                    leftSide.add(StringUtils.join(leftRow, ','));
                    rightSide.add(StringUtils.join(rightRow, ','));
                }
            }
            Collections.sort(leftSide);
            Collections.sort(rightSide);
            return leftSide.equals(rightSide);
        } catch (IOException ignored) {
            return false;
        } finally {
            if (leftReader != null) {
                try {
                    leftReader.close();
                } catch (IOException ignored) {
                }
            }
            if (rightReader != null) {
                try {
                    rightReader.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
