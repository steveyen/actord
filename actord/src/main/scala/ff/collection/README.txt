README for collection classes for actord-d.
-------------------------------------------

2207/04/11 - README created, steve.yen

http://code.google.com/p/actord/source/browse/trunk/actord/src/main/scala/ff/collection

This package includes a basic, persistent, open-source
scala.collection.immutable.SortedMap class.  

It's based on a binary treap data structure, implemented 
by the Treap base class.  The Treap class provides a 
scala.collection.immutable.SortedMap interface and
can be used as a memory-only data structure.

Persistence to storage is provided by a StorageTreap subclass and 
works by appending to the end of a (binary, non-textual) log file.  
Log files are optionally 'rotatable', so you can end up 
with a series of log files to hold all your data.  

During loads, the StorageTreap lazily reads key-value-nodes 
into memory from the storage log(s).  During saves, only changed 
key-value-nodes are appended out to the current storage log. 

Advanced: subsets of data in the StorageTreap can be dynamically 
optionally swizzled out of memory and written to persistent 
storage if you're under memory pressure.  

Usage: just subclass StorageTreap and implement your own key 
and value serialization methods.  Keys and values can be any 
parameterized types.

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

    def errorValue(loc: StorageLoc, error: Object) = ""

    override def mkTreap(r: TreapNode[String, String]): 
        Treap[String, String] =
      new MyStuff(r, s)       
  }

You also have to define a Storage subclass.  Here, I use a 
DirStorage subclass, which provides rotatable log 
files (many log/storage files in a directory).

  import java.io.File

  class MyStorage(someDir: File) extends DirStorage(someDir) {
    def defaultHeader      = "# my stuff file, version 0.0.0\n"
    def defaultPermaMarker = "magic_bytes_123*321_here".getBytes
  }

To load MyStuff from storage...

  val s = new MyStorage(new java.io.File("/tmp/stuff/"))

  val empty = new MyStuff(TreapEmptyNode[String, String], s)

  var mySortedMap: scala.collection.immutable.SortedMap = 
        empty.loadRootNode(s).
              map(root => new MyStuff(root, s)).
              getOrElse(empty)

To manipulate data, use the scala.collection.immutable.SortedMap
interface methods...

  mySortedMap = mySortedMap + ("name" -> "steve")
  mySortedMap = mySortedMap + ("site" -> "www.somesite.com")
  mySortedMap = mySortedMap - "spam"

To save your changes out to storage...

  mySortedMap.asInstanceOf[MyStuff].appendRootNode(s)

For basic, naive crash safety during saves, a save operation is 
not complete until a magic set of bytes, called a permaMarker, 
is appended to the log.  When reloading, the StorageTreap code
scans backwards through the log files for the latest complete 
permaMarker to find the last good root node record of the treap, 
and any partially written log bytes after that last permaMarker 
are truncated away.  In this simple design, we make gigantic 
assumptions about the stability and corruption behavior 
of append-only files.

To 'rotate' (or add) a new log file once the current log file 
gets too big...

  s.pushNextFile

