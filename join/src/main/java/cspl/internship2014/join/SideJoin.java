package cspl.internship2014.join;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

/**
 *
 */
abstract class SideJoin extends JoinBase {

    protected void join(InputStreamReaderMaker outerCsv, InputStreamReaderMaker innerCsv, String joinKey,
                        OutputStreamWriter outputCsv, RecordGeneratorGenerator recordGeneratorGenerator)
            throws IllegalApplicationArgumentException, IllegalDataException, IOException {
        CSVWriter csvWriter = new CSVWriter(outputCsv);

        try {
            // outer chunked
            final RecordProvider recordProvider = new RecordProvider(innerCsv);
            final ChunkProvider chunkProvider = new ChunkProvider(outerCsv.getInputStreamReader());
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
            csvWriter.close();
        }
    }

    protected interface RecordGeneratorGenerator {
        RecordGenerator getRecordGenerator(int left, int right);
    }
}
