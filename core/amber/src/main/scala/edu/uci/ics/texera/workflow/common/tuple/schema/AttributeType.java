package edu.uci.ics.texera.workflow.common.tuple.schema;

import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;
import java.sql.Timestamp;

public enum AttributeType implements Serializable {
    // A field that is indexed but not tokenized: the entire String
    // value is indexed as a single token
    STRING("string", String.class),
    INTEGER("integer", Integer.class),
    LONG("long", Long.class),
    DOUBLE("double", Double.class),
    FLOAT("Float", Float.class),
    BOOLEAN("boolean", Boolean.class),
    TIMESTAMP("timestamp", Timestamp.class),
    ANY("ANY", Object.class);

    private final String name;
    private final Class<?> fieldClass;

    AttributeType(String name, Class<?> fieldClass) {
        this.name = name;
        this.fieldClass = fieldClass;
    }

    @JsonValue
    public String getName() {
        return this.name;
    }

    public Class<?> getFieldClass() {
        return this.fieldClass;
    }

    public static AttributeType getAttributeType(Class<?> fieldClass) {
        if (fieldClass.equals(String.class)) {
            return STRING;
        } else if (fieldClass.equals(Integer.class)) {
            return INTEGER;
        } else if (fieldClass.equals(Double.class)) {
            return DOUBLE;
        } else if (fieldClass.equals(Float.class)) {
            return FLOAT;
        } else if (fieldClass.equals(Boolean.class)) {
            return BOOLEAN;
        } else {
            return ANY;
        }
    }

    @Override
    public String toString() {
        return this.getName();
    }
}
