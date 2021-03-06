package cspl.internship2014.join;

/**
 * Thrown to signalize illegal input data.
 */
class IllegalDataException extends Exception {
    public IllegalDataException(String message) {
        super(message);
    }

    public IllegalDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
