package cspl.internship2014.join;

import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Implementation of right join.
 */
class RightJoin extends SideJoin {

    @Override
    public void join(InputStreamReaderMaker leftCsv, InputStreamReaderMaker rightCsv, String joinKey, OutputStreamWriter outputCsv, SizeHint sizeHint) throws IllegalApplicationArgumentException, IllegalDataException, IOException {
        join(rightCsv, leftCsv, joinKey, outputCsv, new SideJoin.RecordGeneratorGenerator() {
            @Override
            public RecordGenerator getRecordGenerator(int left, int right) {
                return JoinBase.getRecordGeneratorReversed(left);
            }
        });
    }
}
