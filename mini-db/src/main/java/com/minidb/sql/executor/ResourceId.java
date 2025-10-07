package com.minidb.sql.executor;

import java.util.Objects;

public class ResourceId {
    private final String type;
    private final String name;

    private ResourceId(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public static ResourceId table(String tableName) {
        return new ResourceId("table", tableName);
    }

    public static ResourceId key(String tableName, String key) {
        return new ResourceId("key", tableName + ":" + key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceId that = (ResourceId) o;
        return type.equals(that.type) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name);
    }
}
