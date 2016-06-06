package org.sparkRedshift.tutorial

import org.apache.spark.sql.SQLContext
import scaldi.Module

class ScaldiModule(sc:SQLContext) extends Module {

  bind[RedShiftConnector] to new RedShiftConnectorImpl(sc)


}
