package main.java.assignment.bdp.rmit.util;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Pair of words for co-occurence matrix
 *
 * @author Ujjwal Batra
 */
public class Pair implements Writable, WritableComparable<Pair> {
    private Text word1;
    private Text word2;

    public Pair() {
        this.word1 = new Text();
        this.word2 = new Text();
    }

    public Text getWord1() {
        return word1;
    }

    public void setWord1(String word1) {
        this.word1.set(word1);
    }

    public Text getWord2() {
        return word2;
    }

    public void setWord2(String word2) {
        this.word2.set(word2);
    }

    /**
     * Tells is the word exists in the pair.
     *
     * @return true if the word searched for exists in the Pair otherwise false
     */
    public boolean contains(String word) {
        word = word.toLowerCase();
        if (word.equals(word1) || word.equals(word2))
            return true;

        return false;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.word1.write(dataOutput);
        this.word2.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word1.readFields(dataInput);
        this.word2.readFields(dataInput);
    }

    @Override
    public int compareTo(Pair pair) {
        int returnVal = this.word1.compareTo(pair.getWord1());
        if (returnVal != 0) {
            return returnVal;
        }

        return this.word2.compareTo(pair.getWord2());
    }

    @Override
    public int hashCode() {
        int result = this.word1 != null ? this.word1.hashCode() : 0;
        result = 163 * result + (this.word2 != null ? this.word2.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair pair = (Pair) o;

        if (this.word1 != null ? !this.word1.equals(pair.getWord1()) : pair.getWord1() != null) return false;
        if (this.word2 != null ? !this.word2.equals(pair.getWord2()) : pair.getWord2() != null) return false;

        return true;
    }

    @Override
    public String toString() {
        return "{word=[" + this.word1 + "] neighbor=[" + this.word2 + "]}";
    }
}
