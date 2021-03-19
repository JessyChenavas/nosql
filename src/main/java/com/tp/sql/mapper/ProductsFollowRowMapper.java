package com.tp.sql.mapper;

import com.tp.sql.entity.ProductsFollowResponse;
import com.tp.sql.entity.ProductsPurchases;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ProductsFollowRowMapper implements RowMapper<ProductsPurchases> {
    @Override
    public ProductsPurchases mapRow(ResultSet rs, int rowNum) throws SQLException {
        ProductsPurchases p = new ProductsPurchases();

        p.setProductName(rs.getString("name"));
        p.setProductId(rs.getInt("id"));
        p.setPurchases(rs.getInt("nb"));

        return p;
    }
}