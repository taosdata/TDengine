package com.taosdata.jdbc.ws;

/**
 * exception which indicates the websocket is not yet connected (ReadyState.OPEN)
 */
public class WebsocketNotConnectedException extends RuntimeException {

  /**
   * Serializable
   */
  private static final long serialVersionUID = -785314021592982715L;
}
