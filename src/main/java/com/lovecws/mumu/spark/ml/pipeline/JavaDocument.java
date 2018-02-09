package com.lovecws.mumu.spark.ml.pipeline;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: JavaDocument
 * @date 2018-02-07 16:23
 */
public class JavaDocument {

    private long id;
    private String text;

    public JavaDocument(final long id, final String text) {
        this.id = id;
        this.text = text;
    }

    public long getId() {
        return id;
    }

    public void setId(final long id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(final String text) {
        this.text = text;
    }
}
