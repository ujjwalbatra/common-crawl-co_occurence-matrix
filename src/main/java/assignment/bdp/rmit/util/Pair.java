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
        if (!this.word1.equals(pair.word1)) {
            return this.word1.compareTo(pair.word1);
        }

        return this.word2.compareTo(pair.word2);
    }
}
