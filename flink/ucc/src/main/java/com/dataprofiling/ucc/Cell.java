package com.dataprofiling.ucc;

import java.io.Serializable;
import java.util.BitSet;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class Cell implements Comparable<Cell>, Serializable {
    private static final long serialVersionUID = 1L;
    public BitSet columnIndex;
    String value;

    public Cell(BitSet bs, String value) {
        this.columnIndex = bs;
        this.value = value;
    }

    public Cell() {
        this.columnIndex = new BitSet();
        this.value = "This empty string should not appear";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Cell attr = (Cell) obj;
        if (attr.columnIndex.equals(this.columnIndex) && attr.value.equals(this.value)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31).append(columnIndex).append(value).toHashCode();
    }

    @Override
    public String toString() {
        return "[ " + this.columnIndex + ", " + this.value + " ]";
    }

    @Override
    public int compareTo(Cell o) {
        Cell cell = (Cell) o;
        if (cell.columnIndex.equals(this.columnIndex) && cell.value.equals(this.value)) {
            return 1;
        }
        return -1;
    }
}
