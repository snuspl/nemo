package edu.snu.vortex.compiler.eventhandler;

import org.apache.reef.wake.EventHandler;

/**
 * Class for handling events sent from Compiler.
 * @param <T> type of the compiler event to handle.
 */
interface CompilerEventHandler<T extends CompilerEvent> extends EventHandler<T> {
}
