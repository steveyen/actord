package ff.collection;

import junit.framework.*;

public class MainTest extends TestCase {
  public void testMain() {
    // Run actual tests via scala SUnit.
    //
    new StorageTest().main(new String[0]); 
    new TreapStorableTest().main(new String[0]); 
  }
}
