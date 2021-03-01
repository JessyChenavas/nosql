package com.tp.sql.repository;

import com.tp.sql.entity.neo4j.Person;
import com.tp.sql.entity.neo4j.Product;
import org.springframework.data.neo4j.repository.Neo4jRepository;

public interface ProductRepository extends Neo4jRepository<Product, Long> {

}
