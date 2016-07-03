package com.tngtech.bigdata.util;

import java.io.Serializable;
import java.util.Objects;

public class Sale implements Serializable {
    private int    id;
    private String date;

    public Sale(final int id, final String date) {

        this.id = id;
        this.date = date;
    }

    public int getId() {
        return id;
    }


    public String getDate() {
        return date;
    }

    public void setId(final int id) {
        this.id = id;
    }

    public void setDate(final String date) {
        this.date = date;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Sale sale = (Sale) o;
        return id == sale.id &&
               Objects.equals(date, sale.date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, date);
    }

    @Override
    public String toString() {
        return "Sale{" +
               "id=" + id +
               ", date='" + date + '\'' +
               '}';
    }
}
