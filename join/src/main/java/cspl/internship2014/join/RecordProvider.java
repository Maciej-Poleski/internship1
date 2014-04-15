package cspl.internship2014.join;

import au.com.bytecode.opencsv.CSVReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * Wraps all functionality needed to read records from stream (as many times as necessary).
 */
class RecordProvider implements Closeable {
    private final InputStreamReaderMaker csvInputStreamReaderMaker;
    private final String[] header;
    private CSVReader csvReader;

    /**
     * Initializes object with given stream maker.
     *
     * @param csvInputStreamReaderMaker Input stream maker with will be used to make new stream as necessary for
     *                                  {@link cspl.internship2014.join.RecordProvider#reset()}.
     * @throws IOException If there is serious problem with input stream.
     */
    RecordProvider(InputStreamReaderMaker csvInputStreamReaderMaker) throws IOException {
        this.csvInputStreamReaderMaker = csvInputStreamReaderMaker;
        this.csvReader = new CSVReader(csvInputStreamReaderMaker.getInputStreamReader());
        header = csvReader.readNext();
    }

    /**
     * @return Header row of this CSV stream.
     */
    String[] getHeader() {
        return Arrays.copyOf(header, header.length);
    }

    /**
     * Reads and returns next record.
     *
     * @return Next record from CSV input stream.
     * @throws IOException          If there is serious problem with input stream.
     * @throws IllegalDataException If data in input stream is not correct.
     */
    String[] readNextRecord() throws IOException, IllegalDataException {
        String[] row = csvReader.readNext();
        if (row != null && row.length != header.length) {
            throw new IllegalDataException("All rows must contain the same amount of columns.");
        }
        return row;
    }

    /**
     * Resets this record provider. Records will be read from beginning.
     *
     * @throws IOException If there is serious problem with input stream.
     */
    void reset() throws IOException {
        this.csvReader.close();
        this.csvReader = new CSVReader(csvInputStreamReaderMaker.getInputStreamReader());
        csvReader.readNext();
    }

    @Override
    public void close() throws IOException {
        csvReader.close();
    }
}
