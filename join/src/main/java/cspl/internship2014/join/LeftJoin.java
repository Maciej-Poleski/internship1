package cspl.internship2014.join;

import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Implementation of left join.
 */
class LeftJoin extends SideJoin {

    @Override
    public void join(InputStreamReaderMaker leftCsv, InputStreamReaderMaker rightCsv, String joinKey, OutputStreamWriter outputCsv, SizeHint sizeHint) throws IllegalApplicationArgumentException, IllegalDataException, IOException {
        join(leftCsv, rightCsv, joinKey, outputCsv, new RecordGeneratorGenerator() {
            @Override
            public RecordGenerator getRecordGenerator(int left, int right) {
                return JoinBase.getRecordGenerator(right);
            }
        });
    }
}
