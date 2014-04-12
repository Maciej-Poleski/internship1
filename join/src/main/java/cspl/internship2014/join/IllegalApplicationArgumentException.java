package cspl.internship2014.join;

/**
 * Thrown to signalize inappropriate user argument.
 */
class IllegalApplicationArgumentException extends Exception {
    IllegalApplicationArgumentException(String message, Throwable cause) {
        super(message, cause);
    }

    IllegalApplicationArgumentException(String message) {
        super(message);
    }
}
