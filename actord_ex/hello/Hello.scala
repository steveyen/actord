import com.sun.j3d.utils.universe._
import com.sun.j3d.utils.geometry._
import javax.media.j3d._
import javax.vecmath._
import java.awt._

class Hello extends javax.swing.JFrame {
  def createSceneGraph: BranchGroup = {
    val objRoot = new BranchGroup

  	// Create the TransformGroup node and initialize it to the
    // identity. Enable the TRANSFORM_WRITE capability so that
    // our behavior code can modify it at run time. Add it to
    // the root of the subgraph.
    val objTrans = new TransformGroup
    objTrans.setCapability(TransformGroup.ALLOW_TRANSFORM_WRITE)
    objRoot.addChild(objTrans)

    // Create a simple Shape3D node; add it to the scene graph.
    objTrans.addChild(new ColorCube(0.4))

    // Create a new Behavior object that will perform the
    // desired operation on the specified transform and add
    // it into the scene graph.
    val yAxis = new Transform3D
    val rotationAlpha = new Alpha(-1, 4000)

    val rotator =
      new RotationInterpolator(rotationAlpha, objTrans, yAxis,
                               0.0f, (Math.Pi * 2.0).toFloat)
    val bounds =
      new BoundingSphere(new Point3d(0.0, 0.0, 0.0), 100.0)

    rotator.setSchedulingBounds(bounds)
    objRoot.addChild(rotator)

    // Have Java 3D perform optimizations on this scene graph.
    objRoot.compile
    objRoot
  }

  // Initialize the GUI components
  val drawingPanel = new javax.swing.JPanel

  setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE)
  setTitle("Hello")
  drawingPanel.setLayout(new java.awt.BorderLayout)

  drawingPanel.setPreferredSize(new java.awt.Dimension(250, 250))
  getContentPane.add(drawingPanel, java.awt.BorderLayout.CENTER)

  pack

  // Get the preferred graphics configuration for the default screen
  val config = SimpleUniverse.getPreferredConfiguration

  // Create a Canvas3D using the preferred configuration
  val canvas = new Canvas3D(config)

  drawingPanel.add(canvas, java.awt.BorderLayout.CENTER)

  // Create simple universe with view branch
  val univ = new SimpleUniverse(canvas)

  // This will move the ViewPlatform back a bit so the
  // objects in the scene can be viewed.
  univ.getViewingPlatform.setNominalViewingTransform

  // Ensure at least 5 msec per frame (i.e., < 200Hz)
  univ.getViewer.getView.setMinimumFrameCycleTime(5)

  // Create the content branch and add it to the universe
  val scene = createSceneGraph
  
  univ.addBranchGraph(scene)
}

object Hello {
  def main(args: Array[String]) {
    java.awt.EventQueue.invokeLater(new Runnable() {
      def run {
        (new Hello).setVisible(true)
      }
    })
  }
}

Hello.main(null)
