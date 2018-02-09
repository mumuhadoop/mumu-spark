package com.lovecws.mumu.spark.ml.pipeline;

import com.lovecws.mumu.spark.ml.pipeline.MachineLeaningPipeline;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 统计
 * @date 2018-02-07 16:00
 */
public class MachineLeaningPipelineTest {

    private MachineLeaningPipeline pipeline = new MachineLeaningPipeline();

    @Test
    public void transform() {
        pipeline.transform();
    }

    @Test
    public void pipeline() {
        pipeline.pipeline();
    }
}
