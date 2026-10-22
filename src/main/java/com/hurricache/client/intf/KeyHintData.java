package com.hurricache.client.intf;

import java.util.Objects;

public class KeyHintData {
    private final Integer week_hash;
    private final Integer strong_hash;
    private final boolean wasRelayed;

    private KeyHintData(Integer strong_hash, Integer week_hash, boolean wasRelayed) {
        this.strong_hash = strong_hash;
        this.week_hash = week_hash;
        this.wasRelayed = wasRelayed;
    }
    public static KeyHintData of (Integer strong_hash, Integer week_hash, boolean wasRelayed) {
        return new KeyHintData(strong_hash, week_hash, wasRelayed);
    }

    public static KeyHintData of (Integer strong_hash, Integer week_hash) {
        return new KeyHintData(strong_hash, week_hash, false);
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

    public boolean isWasRelayed() {
        return wasRelayed;
    }

    @Override
    public String toString() {
        return "KeyHintData{"
               + "strong_hash="
               + strong_hash
               + ", week_hash="
               + week_hash
               + ", wasRelayed="
               + wasRelayed
               + '}';
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