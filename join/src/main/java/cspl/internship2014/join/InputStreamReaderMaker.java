package cspl.internship2014.join;

import java.io.FileNotFoundException;
import java.io.InputStreamReader;

/**
 * Class can implement this interface to provide method to obtain fresh {@link InputStreamReader} for some file or
 * resource. It is used to enable reading file input more than once.
 */
interface InputStreamReaderMaker {

    /**
     * Creates and returns new {@link InputStreamReader} object for some file or resource.
     *
     * @return New {@link InputStreamReader} object.
     * @throws FileNotFoundException If stream cannot be created because input file was not found.
     */
    InputStreamReader getInputStreamReader() throws FileNotFoundException;
}
