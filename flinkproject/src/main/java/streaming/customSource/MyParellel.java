package streaming.customSource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class MyParellel implements ParallelSourceFunction<Long> {
    private boolean isrun=true;
    private  long count=1l;
    @Override
    public void run(SourceContext<Long> sct) throws Exception {
        while (isrun){
            sct.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isrun=false;
    }
}
