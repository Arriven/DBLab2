import org.junit.Before;
import org.junit.Test;
import java.io.*;

import static junit.framework.TestCase.assertFalse;


public class MainTest {
    private ByteArrayOutputStream outContent;

    @Before
    public void init() throws Exception {
        System.setOut(new PrintStream(outContent = new ByteArrayOutputStream()));
    }

    @Test
    public void getResultsTest() throws IOException {
        Main.main(null);
        assertFalse (outContent.toString().isEmpty());
    }
}