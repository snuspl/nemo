package edu.snu.vortex.runtime.master.eventhandler;

import org.apache.reef.wake.EventHandler;

/**
 * Class for handling events sent from Runtime.
 * @param <T> type of the runtime event to handle.
 */
interface RuntimeEventHandler<T extends RuntimeEvent> extends EventHandler<T> {
}
