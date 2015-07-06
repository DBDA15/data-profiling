package com.dataprofiling.ucc;

import java.io.Serializable;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class Cell implements Serializable {
    private static final long serialVersionUID = 1L;
    public Long columnIndex;
    public String value;

    public Cell(Long columnIndex, String value) {
        this.columnIndex = columnIndex;
        this.value = value;
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

    public String toString() {
        return "[ " + this.columnIndex + ", " + this.value + " ]";
    }

}
