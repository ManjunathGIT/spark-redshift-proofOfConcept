package org.sparkRedshift.tutorial

import org.apache.spark.SparkContext
import scaldi.Module

class ScaldiModule(sc:SparkContext) extends Module {

  bind[RedShiftConnector] to new RedShiftConnectorImpl(sc)


}
