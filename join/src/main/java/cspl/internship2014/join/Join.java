package cspl.internship2014.join;

import java.io.*;

import static cspl.internship2014.join.JoinBase.SizeHint;

public class Join {

    /**
     * java -jar target/join-1.0-SNAPSHOT.jar leftFile rightFile joinKey [joinType]
     *
     * @param args Arguments given on command line.
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            printHelp(System.out, true);
        } else if (args.length < 3 || args.length > 4) {
            printHelp(System.err, true);
        } else {
            try {
                JoinBase join = parseJoinType(args.length == 4 ? args[3] : null);
                File leftFile = new File(args[0]);
                File rightFile = new File(args[1]);
                SizeHint sizeHint = leftFile.length() < rightFile.length() ? SizeHint.LEFT_SHORT : SizeHint.RIGHT_SHORT;
                InputStreamReaderMaker leftCsv = getInputStreamReaderMaker(leftFile);
                InputStreamReaderMaker rightCsv = getInputStreamReaderMaker(rightFile);
                join.join(leftCsv, rightCsv, args[2], new OutputStreamWriter(System.out), sizeHint);
            } catch (IllegalApplicationArgumentException e) {
                System.err.println(e.getMessage());
                printHelp(System.err, false);
            } catch (IllegalDataException e) {
                System.err.println("Input data are broken: " + e.getMessage());
            } catch (IOException e) {
                System.err.println("Exception occurred on I/O: " + e.getMessage());
            }
        }
    }

    /**
     * Create input stream reader maker using given file.
     *
     * @param file File to which stream will be made by maker.
     * @return {@link InputStreamReaderMaker} which will make stream to given file.
     */
    private static InputStreamReaderMaker getInputStreamReaderMaker(final File file) {
        return new InputStreamReaderMaker() {
            @Override
            public InputStreamReader getInputStreamReader() throws FileNotFoundException {
                return new FileReader(file);
            }
        };
    }

    /**
     * Print help message to selected stream.
     *
     * @param stream                Stream for message.
     * @param printBriefDescription Will print application brief info iff {@code true}.
     */
    private static void printHelp(PrintStream stream, boolean printBriefDescription) {
        if (printBriefDescription)
            stream.println("Joins two specified CSV files on given column.");
        stream.println();
        stream.println("Usage: join <file1> <file2> <joinKey> [<joinType>]");
        stream.println();
        stream.println("<file1> and <file2> are files to join.");
        stream.println("<joinKey> is column name. It must exist in both files. Files will be joined on this column.");
        stream.println("[<joinType>] specifies join type. It may be one of:");
        stream.println("    inner - (default) inner join");
        stream.println("    left - left outer join");
        stream.println("    right - right outer join");
    }

    /**
     * Return appropriate {@link JoinBase} implementation based on user argument.
     *
     * @param joinType Join type requested by user.
     * @return {@link JoinBase} implementation based on user request.
     */
    private static JoinBase parseJoinType(String joinType) throws IllegalApplicationArgumentException {
        if (joinType == null || "inner".equalsIgnoreCase(joinType))
            return new InnerJoin();
        else if ("left".equalsIgnoreCase(joinType))
            return new LeftJoin();
        else if ("right".equalsIgnoreCase(joinType))
            return new RightJoin();
        else
            throw new IllegalApplicationArgumentException("Unsupported joint type: " + joinType);
    }
}
