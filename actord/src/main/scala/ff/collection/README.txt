README for collection classes for actord-d.
-------------------------------------------

2207/04/11 - README created, steve.yen

http://code.google.com/p/actord/source/browse/trunk/actord/src/main/scala/ff/collection

This package includes a basic, persistent, open-source
scala.collection.immutable.SortedMap class.  

It's based on a Treap data structure, which implements 
scala.collection.immutable.SortedMap.  You can use the Treap 
on its own, as a memory-only, non-persistent data structure.

Persistence is provided by a StorageTreap subclass and works by 
appending to the end of a (binary, non-textual) log file.  
Log files are optionally 'rotatable', so you can end up 
with a series of log files to hold all your data.  

The data in the StorageTreap can be dynamically, 
partially swizzled in-and-out of memory to persistent storage
if you want to reclaim memory.

To use it, subclass StorageTreap and implement your own key 
and value serialization methods.  Keys and values can be any 
parametrized types.

Here's a String key and String value example...

  import ff.actord.collection._

  class MyStuff(
      override val root: TreapNode[String, String], 
      override val s: Storage) 
      extends StorageTreap[String, String](root, s) 
  {
    def serializeKey(x: String) = x.getBytes
    def unserializeKey(arr: Array[Byte]) = new String(arr)
  
    def serializeValue(x: String, 
                       loc: StorageLoc, 
                       appender: StorageLocAppender) {
      val arr = x.getBytes
      appender.appendArray(arr, 0, arr.length)
    }
    def unserializeValue(loc: StorageLoc, 
                         reader: StorageLocReader) = 
      new String(reader.readArray)

    override def mkTreap(r: TreapNode[String, String]): 
        Treap[String, String] =
      new MyStuff(r, s)       
  }

You also have to define a Storage class.  Here, I use a 
DirStorage subclass, which provides rotatable log 
files (many log/storage files in a directory).

  import java.io.File

  class MyStorage(someDir: File) extends DirStorage(someDir) {
    def defaultHeader      = "# my stuff file, version 0.0.0\n"
    def defaultPermaMarker = "magic_bytes_123*321_here".getBytes
  }

To load MyStuff from storage...

  val s = new MyStorage(new java.io.File("/tmp/stuff/"))

  val emptyStuff = new MyStuff(TreapEmptyNode[String, String], s)
  var myStuff = emptyStuff.loadRootNode(s).
                           map(root => new MyStuff(root, s)).
                           getOrElse(emptyStuff)

  var mySortedMap: scala.collection.immutable.SortedMap = myStuff

To manipulate data, use the scala.collection.immutable.SortedMap
interface methods...

  mySortedMap = mySortedMap + ("name" -> "steve")
  mySortedMap = mySortedMap + ("site" -> "www.somesite.com")
  mySortedMap = mySortedMap - "spam"

To save it all out to storage...

  mySortedMap.asInstanceOf[MyStuff].appendRootNode(s)

Successive calls to appendRootNode() will only append
changed/delta key-value information.

To 'rotate' (or add) a new log file once the current log file 
gets too big...

  s.pushNextFile

