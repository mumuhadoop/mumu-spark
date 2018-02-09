package com.lovecws.mumu.spark.ml.pipeline;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: JavaLabeledDocument
 * @date 2018-02-07 16:22
 */
public class JavaLabeledDocument {

    private long label;
    private String text;
    private double score;

    public JavaLabeledDocument(final long label, final String text, final double score) {
        this.label = label;
        this.text = text;
        this.score = score;
    }

    public long getLabel() {
        return label;
    }

    public void setLabel(final long label) {
        this.label = label;
    }

    public String getText() {
        return text;
    }

    public void setText(final String text) {
        this.text = text;
    }

    public double getScore() {
        return score;
    }

    public void setScore(final double score) {
        this.score = score;
    }
}
