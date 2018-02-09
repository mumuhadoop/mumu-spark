package com.lovecws.mumu.spark.ml.filtering;

import java.io.Serializable;

public class Rating implements Serializable {
    private int userId;
    private int movieId;
    private float rating;
    private long timestamp;

    public Rating() {
    }

    public Rating(int userId, int movieId, float rating, long timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    public int getUserId() {
        return userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public float getRating() {
        return rating;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static Rating parse(String str, String regex) {
        String[] fields = str.split(regex);
        if (fields.length < 3) {
            throw new IllegalArgumentException("Each line must contain 3 fields");
        }
        int userId = Integer.parseInt(fields[0]);
        int movieId = Integer.parseInt(fields[1]);
        float rating = Float.parseFloat(fields[2]);
        long timestamp = 0;
        if (fields.length == 4) {
            timestamp = Long.parseLong(fields[3]);
        }
        return new Rating(userId, movieId, rating, timestamp);
    }

    public static Rating parseRating(String str) {
        return parse(str, "::");
    }

    public static Rating parseRatingCsv(String str) {
        return parse(str, ",");
    }
}