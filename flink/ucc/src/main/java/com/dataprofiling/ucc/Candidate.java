package com.dataprofiling.ucc;

import java.io.Serializable;
import java.util.BitSet;

import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * in the first round candidate equals columnIndex
 * @author Jaqueline
 *
 */
public class Candidate extends BitSet implements Comparable<Candidate>, Serializable{
    private static final long serialVersionUID = -2331780728569111267L;
    public Candidate(BitSet b) {
        super(b.length());
        for (int i = 0; i <b.length();i++) {
            this.set(i, b.get(i));
        }
    }
    
    public Candidate() {
        super();
    }

    @Override
    public int compareTo(Candidate o) {
        if (!(o instanceof BitSet)) {
            return -1;
        }
        BitSet bitSet = (BitSet) o;
        if (equals(bitSet)) {
            return 0;
        }
        return 1;
    }
    

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Candidate c= (Candidate) obj;
        if (length() != c.length()) return false;
        for (int i = 0; i< c.length(); i++) {
            if (get(i) != c.get(i)){
                return false;
            }
        }
        return true;
    }
    
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31).append(this.toString()).toHashCode();
    }

}
