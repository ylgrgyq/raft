package raft.server;

import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Author: ylgrgyq
 * Date: 18/4/8
 */
public class StupidTest {
    private ExecutorService executor = Executors.newFixedThreadPool(6);

    private static ArrayList<Integer> sentinel = new ArrayList<>();
    @Test
    public void test111() throws Exception {
        for (int j = 0; j < 1000000; j++) {
            Delay a = new Delay(new IFn() {
                @Override
                public Object invoke() {
                    return new ArrayList<Integer>();
                }
            });

            CountDownLatch latch = new CountDownLatch(6);
            for (int i = 0; i < 6; i++) {
                executor.submit(() -> {
                    latch.countDown();
                    if (a.deref() == sentinel) {
                        System.out.println("Found it");
                    }
                });
            }

            latch.await();
        }
    }

    public class Delay{
        Object val;
        Throwable exception;
        IFn fn;

        public Delay(IFn fn){
            this.fn = fn;
            this.val = sentinel;
            this.exception = null;
        }

        public Object deref() {
            if(fn != null) {
                synchronized(this) {
                    //double check
                    if(fn != null) {
                        try {
                            val = fn.invoke();
                        } catch(Throwable t) {
                            exception = t;
                        }
                        fn = null;
                    }
                }
            }
            if(exception != null)
                throw new RuntimeException(exception);

            return val;
        }
    }

    interface IFn {
        Object invoke();
    }

}
