package com.tp.sql.repository;

import com.tp.sql.entity.neo4j.Person;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.neo4j.repository.ReactiveNeo4jRepository;
import org.springframework.stereotype.Repository;

public interface PersonRepository extends Neo4jRepository<Person, Long> {

    Person findByFirstname(String name);

}
