package com.wjy.flink.wc.vo;

import java.io.Serializable;

public class Wc implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 2995909841823135711L;

    public Wc() {
        super();
    }

    public Wc(String word, int frequency) {
        super();
        this.word = word;
        this.frequency = frequency;
    }

    private String word; // 单词
    private int frequency; // 出现频率

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return String.format("Wc [word=%s, frequency=%s]", word, frequency);
    }

}
