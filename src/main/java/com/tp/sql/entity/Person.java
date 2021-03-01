package com.tp.sql.entity;

import lombok.Data;

@Data
public class Person {
    private long id;
    private String firstName, lastName;

    public Person(long id, String firstName, String lastName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
    }
}
