package cspl.internship2014.join;

import au.com.bytecode.opencsv.CSVReader;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Providers records from CSV in chunks.
 */
class ChunkProvider implements Closeable {
    private final String[] header;
    private final CSVReader csvReader;

    /**
     * Initializes this chunk provider with given {@link InputStreamReader}
     *
     * @param inputStreamReader Input stream from which records will be read.
     * @throws IOException If there is serious problem with input stream.
     */
    ChunkProvider(InputStreamReader inputStreamReader) throws IOException {
        csvReader = new CSVReader(inputStreamReader);
        header = csvReader.readNext();
    }

    /**
     * @return Header row of this CSV stream.
     */
    String[] getHeader() {
        return Arrays.copyOf(header, header.length);
    }

    /**
     * Prepares map with chunk of data from join key to list of rows (tuples). {@code maxSizeHint} will be used as hint
     * for chunk size (total length of strings in collected rows should be approximately {@code maxSizeHint}). As a keys
     * will be used strings from column {@code column}.
     *
     * @param maxSizeHint Size hint. Total length of collected rows should be approximated by this number.
     * @param column      Keys will be taken from this column.
     * @return Mapping from selected column values to rows.
     * @throws IOException          If there is serious problem with input stream.
     * @throws IllegalDataException If data in input stream is not correct.
     */
    Map<String, List<String[]>> getChunk(long maxSizeHint, int column) throws IOException, IllegalDataException {
        HashMap<String, List<String[]>> result = new HashMap<String, List<String[]>>();
        while (maxSizeHint > 0) {
            String[] row = csvReader.readNext();
            if (row == null) {
                break;
            }
            if (row.length != header.length) {
                throw new IllegalDataException("All rows must contain the same amount of columns.");
            }
            for (String s : row) {
                maxSizeHint -= s.length();
            }
            List<String[]> tuples = result.get(row[column]);
            if (tuples != null) {
                tuples.add(row);
            } else {
                tuples = new ArrayList<String[]>();
                tuples.add(row);
                result.put(row[column], tuples);
            }
        }
        return result;
    }

    /**
     * Close underlying CSV reader.
     *
     * @throws IOException If there is serious problem with input stream.
     */
    @Override
    public void close() throws IOException {
        csvReader.close();
    }
}
