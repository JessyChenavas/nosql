package com.tp.sql.entity;

import lombok.Data;

@Data
public class DatabaseStatus {
    private Long personsNumber;
    private Long productsNumber;
    private Long friendsNumber;
    private Long purchasesNumber;
}
