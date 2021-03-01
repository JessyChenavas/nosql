package com.tp.sql.entity;

import lombok.Data;

import java.util.List;

@Data
public class ProductsFollowResponse extends Response {
    private List<ProductsPurchases> productsPurchases;
}
