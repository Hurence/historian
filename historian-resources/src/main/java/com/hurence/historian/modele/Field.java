package com.hurence.historian.modele;

public class Field {
    private final String name;
    private final String type;
    private final Boolean multivalued;
    private final Boolean indexed;
    private final Boolean required;
    private final Boolean stored;

    private Field(String name, String type,
                 boolean multivalued, boolean indexed,
                 boolean required, boolean stored) {
        this.name = name;
        this.type = type;
        this.multivalued = multivalued;
        this.indexed = indexed;
        this.required = required;
        this.stored = stored;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public Boolean getMultivalued() {
        return multivalued;
    }

    public Boolean getIndexed() {
        return indexed;
    }

    public Boolean getRequired() {
        return required;
    }

    public Boolean getStored() {
        return stored;
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", multivalued=" + multivalued +
                ", indexed=" + indexed +
                ", required=" + required +
                ", stored=" + stored +
                '}';
    }

    public static class Builder {
        private String name;
        private String type;
        private boolean multivalued = false;
        private boolean indexed = true;
        private boolean required = false;
        private boolean stored = true;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withMultivalued(boolean multivalued) {
            this.multivalued = multivalued;
            return this;
        }

        public Builder withIndexed(boolean indexed) {
            this.indexed = indexed;
            return this;
        }

        public Builder withRequired(boolean required) {
            this.required = required;
            return this;
        }

        public Builder withStored(boolean stored) {
            this.stored = stored;
            return this;
        }

        public Field build() {
            return new Field(name, type, multivalued, indexed, required, stored);
        }
    }

}
