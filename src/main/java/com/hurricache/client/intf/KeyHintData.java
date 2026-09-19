package com.hurricache.client.intf;

import java.util.Objects;

public class KeyHintData {
    private final Integer week_hash;
    private final Integer strong_hash;

    private KeyHintData(Integer strong_hash, Integer week_hash) {
        this.strong_hash = strong_hash;
        this.week_hash = week_hash;
    }
    public static KeyHintData of (Integer strong_hash, Integer week_hash) {
        return new KeyHintData(strong_hash, week_hash);
    }

    public Integer getStrong_hash() {
        return strong_hash;
    }

    public Integer getWeek_hash() {
        return week_hash;
    }
    public boolean hasStrongHash() {
        return strong_hash != null;
    }
    public boolean hasWeekHash(){
        return week_hash != null;
    }

    @Override
    public String toString() {
        return "KeyHint{" + "strong_hash=" + strong_hash + ", week_hash=" + week_hash + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyHintData keyHint = (KeyHintData) o;
        return Objects.equals(week_hash, keyHint.week_hash) && Objects.equals(strong_hash, keyHint.strong_hash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(week_hash, strong_hash);
    }
}