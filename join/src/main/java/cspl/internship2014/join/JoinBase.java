package cspl.internship2014.join;

import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * All join implementations share this base class and functionality.
 */
abstract class JoinBase {
    /**
     * Used as limit of chunk size. (Because of implicit memory constraints)
     */
    static final long MAX_SIZE_HINT = 16 * 1024 * 1024;

    /**
     * Provides standard {@link RecordGenerator} implementation (removes column at index {@code rightKey} and
     * "concatenates" rest as is.
     *
     * @param rightKey Column index at "right" side which will be removed from results.
     * @return {@link RecordGenerator} implementation.
     */
    protected static RecordGenerator getRecordGenerator(final int rightKey) {
        return new RecordGenerator() {
            @Override
            public String[] generate(String[] leftRecord, String[] rightRecord) {
                String[] result = new String[leftRecord.length + rightRecord.length - 1];
                System.arraycopy(leftRecord, 0, result, 0, leftRecord.length);
                System.arraycopy(rightRecord, 0, result, leftRecord.length, rightKey);
                System.arraycopy(rightRecord, rightKey + 1, result, leftRecord.length + rightKey, rightRecord.length - rightKey - 1);
                return result;
            }
        };
    }

    /**
     * Provides "reversed" {@link RecordGenerator} implementation (swaps left and right sides).
     *
     * @param leftKey Column index at "left" side which will be removed from results.
     * @return {@link RecordGenerator} implementation.
     */
    protected static RecordGenerator getRecordGeneratorReversed(final int leftKey) {
        return new RecordGenerator() {
            private final RecordGenerator impl = getRecordGenerator(leftKey);

            @Override
            public String[] generate(String[] leftRecord, String[] rightRecord) {
                return impl.generate(rightRecord, leftRecord);
            }
        };
    }

    /**
     * Perform join operation.
     *
     * @param leftCsv   Left CSV input stream maker.
     * @param rightCsv  Right CSV input stream maker.
     * @param joinKey   Key (column) to join on.
     * @param outputCsv Output CSV stream. Result of this operation will be written to this stream.
     * @param sizeHint  Hint for implementation. Which stream is probably shorter. May be ignored.
     * @throws IllegalApplicationArgumentException
     *                              If some argument is illegal (key is not column name in both CSV streams).
     * @throws IllegalDataException If input data (CSV stream) is not correct.
     * @throws IOException          If there is serious problem with closing output stream.
     */
    abstract void join(InputStreamReaderMaker leftCsv, InputStreamReaderMaker rightCsv, String joinKey, OutputStreamWriter outputCsv, SizeHint sizeHint)
            throws IllegalApplicationArgumentException, IllegalDataException, IOException;

    /**
     * Provides hint for implementation (which stream is probably shorter).
     */
    enum SizeHint {
        LEFT_SHORT,
        RIGHT_SHORT
    }

    /**
     * Last step in process of generating join result is to combine tuples (from left and right relations). This
     * interface is used to provide implementation of this functionality appropriate for given join operation.
     */
    protected interface RecordGenerator {
        /**
         * Generates result record from parts.
         *
         * @param leftRecord  Left record
         * @param rightRecord Right record
         * @return Result record built using given records.
         */
        String[] generate(String[] leftRecord, String[] rightRecord);
    }
}
