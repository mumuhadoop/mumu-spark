package com.lovecws.mumu.spark.sql.aggregate;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 用户自定义函数
 * @date 2018-02-06 15:30
 */
public class MyAverage extends UserDefinedAggregateFunction {

    private StructType inputSchema;
    private StructType bufferSchema;

    public MyAverage() {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
        inputSchema = DataTypes.createStructType(inputFields);

        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
        bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
        bufferSchema = DataTypes.createStructType(bufferFields);
    }

    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(final MutableAggregationBuffer mutableAggregationBuffer) {
        mutableAggregationBuffer.update(0, 0L);
        mutableAggregationBuffer.update(1, 0L);
    }

    @Override
    public void update(final MutableAggregationBuffer buffer, final Row input) {
        if (!input.isNullAt(0)) {
            long updatedSum = buffer.getLong(0) + input.getLong(0);
            long updatedCount = buffer.getLong(1) + 1;
            buffer.update(0, updatedSum);
            buffer.update(1, updatedCount);
        }
    }

    @Override
    public void merge(final MutableAggregationBuffer buffer, final Row input) {
        long mergedSum = buffer.getLong(0) + input.getLong(0);
        long mergedCount = buffer.getLong(1) + input.getLong(1);
        buffer.update(0, mergedSum);
        buffer.update(1, mergedCount);
    }

    @Override
    public Object evaluate(final Row row) {
        return ((double) row.getLong(0)) / row.getLong(1);
    }
}
