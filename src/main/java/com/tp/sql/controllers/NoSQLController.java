package com.tp.sql.controllers;

import com.tp.sql.entity.Response;
import com.tp.sql.entity.neo4j.Person;
import com.tp.sql.entity.neo4j.Product;
import com.tp.sql.repository.PersonRepository;
import com.tp.sql.repository.ProductRepository;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.neo4j.driver.*;

import javax.transaction.Transactional;

import static org.neo4j.driver.Values.parameters;


@RestController
@RequestMapping("/api/nosql")
public class NoSQLController {

    private final Driver driver;

    private final PersonRepository personRepository;
    private final ProductRepository productRepository;
    private List<String> firstNames = Arrays.asList("Virginie", "Bastien", "Jessy", "Dorian", "Guillaume", "Alexis", "Bertrand");
    private List<String> lastNames = Arrays.asList("Sansone", "Girard-Blanc", "Chenavas", "Dignac", "Lagay", "Pouzeratte", "Masse");
    private List<String> products = Arrays.asList("Frigo", "Canapé", "Tabouret", "Lot de poêles", "Lit", "Nourriture", "Gel douche", "Concombre");

    public NoSQLController(PersonRepository personRepository, ProductRepository productRepository, Driver driver) {
        this.personRepository = personRepository;
        this.productRepository = productRepository;
        this.driver = driver;


    }

    @PostMapping(value = "/test", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> test(@RequestParam("number") Integer number) {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
        Session session = driver.session();
        Response response = new Response();

        Random rand = new Random();
        List<String> randomNames = new ArrayList<>();
        long startTime = System.nanoTime();

        int k = 0;
        for (int i = 0; i < (number / 1000); i++) {
            int count = 1000;
            long begin = System.currentTimeMillis();

            for (int j = 0; j < count; j++) {
                String firstName = firstNames.get(rand.nextInt(firstNames.size()));
                String lastName = lastNames.get(rand.nextInt(lastNames.size()));
                randomNames.add(firstName + " " + lastName);
            }

            try (Transaction tx = session.beginTransaction()) {
                for (int y = 0; y < count; y++) {
                    tx.run("CREATE (a:Person {id:'" + k + "', name:'" + randomNames.get(y) + "'})");
                    k++;
                }
                tx.commit();
            }
            long end = System.currentTimeMillis();
            System.out.print("Inserting " + (double) count / ((double) (end - begin) / count) + " nodes per second.\n");

        }

        long stopTime = System.nanoTime();
        response.setExecution_time((stopTime - startTime) / Math.pow(10, 6));

        session.close();
        driver.close();
        return ResponseEntity.ok(response);

    }

    @PostMapping(value = "/test2", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> test2() {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
        Session session = driver.session();
        Response response = new Response();

        Result result;
        Integer test;
        try (Transaction tx = session.beginTransaction()) {
            result = tx.run("MATCH (n) RETURN count(n) as count");
            test = result.single().get(0).asInt();
        }


        session.close();
        driver.close();
        return ResponseEntity.ok(response);

    }

    @Transactional
    @PostMapping(value = "/insert/persons", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertPersons(@RequestParam("number") Integer number) {
        Response response = new Response();
        try {
            Random rand = new Random();
            List<Person> personList = new ArrayList<>();

            for (int i = 0; i < number; i++) {
                String firstName = firstNames.get(rand.nextInt(firstNames.size()));
                String lastName = lastNames.get(rand.nextInt(lastNames.size()));
                personList.add(new Person(firstName, lastName));
            }

            for (int i = 0; i < number; i++) {
                Person person = personList.get(i);
                // TODO: RANDOM
                for (int nbFriends = 0; nbFriends < 10; nbFriends++) {
                    Person friend = personList.get(rand.nextInt(personList.size()));
                    if (friend != person) {
                        person.follows(friend);
                    }
                }
                personRepository.save(person);

            }

            long startTime = System.nanoTime();
            // personRepository.saveAll(personList);
            long stopTime = System.nanoTime();
            response.setExecution_time((stopTime - startTime) / Math.pow(10, 6));
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(response);

    }

    @PostMapping(value = "/insert/products", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertProducts() {
        Response response = new Response();
        try {
            List<Product> productList = new ArrayList<>();
            products.forEach(name -> {
                productList.add(new Product(name));
            });

            long startTime = System.nanoTime();
            // Uses JdbcTemplate's batchUpdate operation to bulk load data
            productRepository.saveAll(productList);
            long stopTime = System.nanoTime();
            response.setExecution_time((stopTime - startTime) / Math.pow(10, 6));
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(response);
    }
}
