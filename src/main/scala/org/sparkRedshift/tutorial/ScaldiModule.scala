package org.sparkRedshift.tutorial

import scaldi.Module

class ScaldiModule extends Module {

  bind[RedShiftConnector] to new RedShiftConnectorImpl
}
