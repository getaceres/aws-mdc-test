package org.example.aws;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

public class MdcPropagationPool extends ThreadPoolExecutor {

  /**
   *
   */
  private static final long serialVersionUID = 8429818686441433792L;

  public MdcPropagationPool(int numThreads) {
    super(numThreads, numThreads, 30L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
  }

  @Override
  public void execute(Runnable task) {
    super.execute(new RunnableWrapperWithMdc(task));
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return super.submit(new CallableWrapperWithMdc<>(task));
  }

  @Override
  public Future<?> submit(Runnable task) {
    return super.submit(new RunnableWrapperWithMdc(task));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return super.submit(new RunnableWrapperWithMdc(task), result);
  }

  /**
   * Helper {@link Runnable} class that transfers {@link MDC} context values from
   * the origin thread to the execution thread
   *
   * @author crhx7117
   *
   */
  @Slf4j
  static class RunnableWrapperWithMdc implements Runnable {
    private final Runnable wrapped;
    private final Map<String, String> map;

    public RunnableWrapperWithMdc(Runnable wrapped) {
      this.wrapped = wrapped;
      // we are in the origin thread: capture the MDC
      Map<String, String> map1 = MDC.getCopyOfContextMap();
      if (map1 == null) {
        log.error("Received a null MDC map");
        map1 = new HashMap<>();
      }
      map = map1;
    }

    @Override
    public void run() {
      // we are in the execution thread: set the original MDC
      MDC.setContextMap(map);
      wrapped.run();
    }
  }
  /**
   * Helper {@link Callable} class that transfers {@link MDC} context values from
   * the origin thread to the execution thread
   *
   * @author crhx7117
   *
   */
  @Slf4j
  static class CallableWrapperWithMdc<T> implements Callable<T> {
    private final Callable<T> wrapped;
    private final Map<String, String> map;

    public CallableWrapperWithMdc(Callable<T> wrapped) {
      this.wrapped = wrapped;
      // we are in the origin thread: capture the MDC
      Map<String, String> map1 = MDC.getCopyOfContextMap();
      if (map1 == null) {
        log.error("Received a null MDC map");
        map1 = new HashMap<>();
      }
      map = map1;
    }

    @Override
    public T call() throws Exception {
      // we are in the execution thread: set the original MDC
      MDC.setContextMap(map);
      return wrapped.call();
    }
  }


}
