package cspl.internship2014.join;

import au.com.bytecode.opencsv.CSVWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Implementation of inner join.
 */
class InnerJoin extends JoinBase {

    @Override
    public void join(InputStreamReaderMaker leftCsv, InputStreamReaderMaker rightCsv, String joinKey, OutputStreamWriter outputCsv, SizeHint sizeHint) throws IllegalApplicationArgumentException, IllegalDataException, IOException {
        CSVWriter csvWriter = new CSVWriter(outputCsv);
        if (sizeHint != SizeHint.LEFT_SHORT) {
            // We divide into chunks left side.
            // This way we can reuse "left chunking" implementation to divide into chunks right side if hint suggests
            // that it would improve I/O.
            InputStreamReaderMaker t = leftCsv;
            leftCsv = rightCsv;
            rightCsv = t;
        }
        RecordProvider recordProvider = null;
        ChunkProvider chunkProvider = null;
        try {
            // left chunked
            recordProvider = new RecordProvider(rightCsv);
            chunkProvider = new ChunkProvider(leftCsv.getInputStreamReader());
            final int leftKey = Arrays.asList(chunkProvider.getHeader()).indexOf(joinKey);
            final int rightKey = Arrays.asList(recordProvider.getHeader()).indexOf(joinKey);
            if (leftKey == -1 || rightKey == -1) {
                throw new IllegalDataException("Selected join key (" +
                        joinKey + ") does not exist in at least one of input files.");
            }
            final RecordGenerator generator = sizeHint == SizeHint.LEFT_SHORT ? getRecordGenerator(rightKey) : getRecordGeneratorReversed(leftKey);
            csvWriter.writeNext(generator.generate(chunkProvider.getHeader(), recordProvider.getHeader()));
            for (Map<String, List<String[]>> chunk = chunkProvider.getChunk(MAX_SIZE_HINT, leftKey);
                 !chunk.isEmpty();
                 chunk = chunkProvider.getChunk(MAX_SIZE_HINT, leftKey)) {
                for (String[] record = recordProvider.readNextRecord(); record != null; record = recordProvider.readNextRecord()) {
                    final List<String[]> leftRecords = chunk.get(record[rightKey]);
                    if (leftRecords != null) {
                        for (String[] lr : leftRecords) {
                            csvWriter.writeNext(generator.generate(lr, record));
                        }
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
}
