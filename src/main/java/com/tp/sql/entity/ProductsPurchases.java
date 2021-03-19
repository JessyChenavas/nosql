package com.tp.sql.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductsPurchases {
    private String productName;
    private int productId;
    private int purchases;
}
