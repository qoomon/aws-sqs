package com.mytaxi.amazonaws.sqs;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UtilClass to continuously poll from amazon message queue
 *
 * @author bengtbrodersen
 *
 */
public class WorkerExecutor
{

    static final Logger           LOG = LoggerFactory.getLogger(WorkerExecutor.class);

    private final ExecutorService executorService;
    private final Runnable        worker;
    private final int             workerCount;
    private final Handler         handler;




    public WorkerExecutor(final int workerCount, final Handler handler)
    {
        this(Executors.newFixedThreadPool(workerCount), workerCount, handler);
    }




    public WorkerExecutor(final ExecutorService executorService, final int workerCount, final Handler handler)
    {
        super();
        this.workerCount = workerCount;
        this.executorService = executorService;
        this.handler = handler;

        this.worker = new Runnable()
        {

            @Override
            public void run()
            {
                while (!WorkerExecutor.this.executorService.isShutdown())
                {
                    try
                    {
                        WorkerExecutor.this.handler.run();
                    }
                    catch (final Throwable e)
                    {
                        LOG.error("uncought exception", e);
                    }
                }
            }
        };
    }




    public void start()
    {
        // initialize workers
        for (int i = 0; i < this.workerCount; i++)
        {
            this.executorService.submit(this.worker);
        }
    }




    public void releaseExternalResources() throws InterruptedException
    {
        this.executorService.shutdown();
        this.executorService.awaitTermination(30, TimeUnit.SECONDS);
    }




    public interface Handler extends Runnable
    {

        @Override
        public void run();
    }

}
