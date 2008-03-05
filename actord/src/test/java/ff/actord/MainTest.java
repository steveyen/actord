package ff.actord;

import junit.framework.*;

public class MainTest extends TestCase {
  public void testMain() {
    // Run some in-memory tests via scala SUnit.
    //
    new MServerTest().main(new String[0]); 

    // Fire up a real server listening on a port.
    //
    Main.main(new String[0]);

    // Fire up a real client to exercise the server.
    //
    new MServerTestClient().main(new String[0]);
  }
}
