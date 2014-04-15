package cspl.internship2014.join;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 * Base implementation of side join.
 */
abstract class SideOuterJoin extends JoinBase {

    /**
     * "Side" outer join implementation. Must be parametrized by {@link RecordGeneratorGenerator} to adjust "outer side"
     * position.
     *
     * @param outerCsv                 Outer side CSV input stream maker.
     * @param innerCsv                 Inner side CSV input stream maker.
     * @param joinKey                  Key (column) to join on.
     * @param outputCsv                Output CSV stream. Result of this operation will be written to this stream.
     * @param recordGeneratorGenerator {@link RecordGeneratorGenerator} implementation which selects order od outer and
     *                                 inner side.
     * @throws IllegalApplicationArgumentException
     *                              If some argument is illegal (key is not column name in both CSV streams).
     * @throws IllegalDataException If input data (CSV stream) is not correct.
     * @throws IOException          If there is serious problem with closing output stream.
     */
    protected void join(InputStreamReaderMaker outerCsv, InputStreamReaderMaker innerCsv, String joinKey,
                        OutputStreamWriter outputCsv, RecordGeneratorGenerator recordGeneratorGenerator)
            throws IllegalApplicationArgumentException, IllegalDataException, IOException {
        CSVWriter csvWriter = new CSVWriter(outputCsv);
        RecordProvider recordProvider = null;
        ChunkProvider chunkProvider = null;
        try {
            // outer chunked
            recordProvider = new RecordProvider(innerCsv);
            chunkProvider = new ChunkProvider(outerCsv.getInputStreamReader());
            final int leftKey = Arrays.asList(chunkProvider.getHeader()).indexOf(joinKey);
            final int rightKey = Arrays.asList(recordProvider.getHeader()).indexOf(joinKey);
            if (leftKey == -1 || rightKey == -1) {
                throw new IllegalDataException("Selected join key (" +
                        joinKey + ") does not exist in at least one of input files.");
            }
            final RecordGenerator generator = recordGeneratorGenerator.getRecordGenerator(leftKey, rightKey);
            csvWriter.writeNext(generator.generate(chunkProvider.getHeader(), recordProvider.getHeader()));
            String[] nullRow = new String[recordProvider.getHeader().length];
            Arrays.fill(nullRow, "null");
            for (Map<String, List<String[]>> chunk = chunkProvider.getChunk(MAX_SIZE_HINT, leftKey);
                 !chunk.isEmpty();
                 chunk = chunkProvider.getChunk(MAX_SIZE_HINT, leftKey)) {
                Set<String> unusedKeys = new HashSet<String>(chunk.keySet());
                for (String[] record = recordProvider.readNextRecord(); record != null; record = recordProvider.readNextRecord()) {
                    final List<String[]> leftRecords = chunk.get(record[rightKey]);
                    unusedKeys.remove(record[rightKey]);
                    if (leftRecords != null) {
                        for (String[] lr : leftRecords) {
                            csvWriter.writeNext(generator.generate(lr, record));
                        }
                    }
                }
                for (String key : unusedKeys) {
                    final List<String[]> leftRecords = chunk.get(key);
                    nullRow[rightKey] = key;
                    for (String[] lr : leftRecords) {
                        csvWriter.writeNext(generator.generate(lr, nullRow));
                    }
                }
                recordProvider.reset();
            }

        } catch (FileNotFoundException e) {
            throw new IllegalApplicationArgumentException("File not found: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new IllegalDataException(e.getMessage(), e);
        } finally {
            if (recordProvider != null) {
                recordProvider.close();
            }
            if (chunkProvider != null) {
                chunkProvider.close();
            }
            csvWriter.close();
        }
    }

    /**
     * Derived class provides implementation of this interface to
     * {@link SideOuterJoin#join(InputStreamReaderMaker, InputStreamReaderMaker, String, java.io.OutputStreamWriter, SideOuterJoin.RecordGeneratorGenerator)}
     * method.
     */
    protected interface RecordGeneratorGenerator {
        /**
         * This method has to pick appropriate {@link RecordGenerator} implementation and one of given arguments.
         *
         * @param left  Index of join key column assuming left side is inner.
         * @param right Index of join key column assuming right side is inner.
         * @return {@link RecordGenerator} implementation.
         */
        RecordGenerator getRecordGenerator(int left, int right);
    }
}
