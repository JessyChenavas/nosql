package com.tp.sql.controllers;

import com.tp.sql.entity.DatabaseStatus;
import com.tp.sql.entity.ProductsFollowResponse;
import com.tp.sql.entity.ProductsPurchases;
import com.tp.sql.entity.Response;
import org.apache.commons.lang3.RandomStringUtils;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

import org.neo4j.driver.*;

@RestController
@RequestMapping("/api/nosql")
public class NoSQLController {

    Random rand = new Random();

    private final List<String> firstNames = Arrays.asList("Virginie", "Bastien", "Jessy", "Dorian", "Guillaume", "Alexis", "Bertrand");
    private final List<String> lastNames = Arrays.asList("Sansone", "Girard-Blanc", "Chenavas", "Dignac", "Lagay", "Pouzeratte", "Masse");

    public NoSQLController() {
    }

    @GetMapping(value = "/status", produces = "application/json; charset=utf-8")
    public ResponseEntity<DatabaseStatus> status() {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
        Session session = driver.session();
        DatabaseStatus status = new DatabaseStatus();

        try (Transaction tx = session.beginTransaction()) {
            Result result = tx.run("MATCH (n:Person) RETURN count(n) as count");
            status.setPersonsNumber(result.single().get(0).asLong());
            result = tx.run("MATCH (n:Product) RETURN count(n) as count");
            status.setProductsNumber(result.single().get(0).asLong());
            result = tx.run("MATCH ()-[r:Purchase]->() RETURN count(r) as count");
            status.setPurchasesNumber(result.single().get(0).asLong());
            result = tx.run("MATCH ()-[r:Follow]->() RETURN count(r) as count");
            status.setFriendsNumber(result.single().get(0).asLong());
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(status);
    }

    @PostMapping(value = "/insert/persons", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertPersons(@RequestParam("number") Integer number, @RequestParam("batchSize") Integer batchSize) {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
        Session session = driver.session();
        Response response = new Response();
        long elapsedTime = 0;

        Map<String, Object> params = new HashMap<>();
        List<Map<String, Object>> maps = new ArrayList<>();
        int id = 0;
        for (int i = 0; i < (number / batchSize); i++) {
            for (int j = 0; j < batchSize; j++) {
                maps.add(Map.of(
                        "firstname", firstNames.get(rand.nextInt(firstNames.size())),
                        "lastname", lastNames.get(rand.nextInt(lastNames.size())),
                        "id", id++
                ));
            }

            params.put("props", maps);
            try (Transaction tx = session.beginTransaction()) {
                String query = "UNWIND $props AS properties " +
                        " CREATE (n:Person)" +
                        " SET n = properties";
                long begin = System.currentTimeMillis();
                tx.run(query, params);
                tx.commit();
                long end = System.currentTimeMillis();
                elapsedTime += (end - begin);
                System.out.print("Inserting " + (((double) batchSize / (double) (end - begin)) * 1000) + " nodes per second.\n");
            }

            maps = new ArrayList<>();
        }
        response.setExecutionTime((elapsedTime) / Math.pow(10, 3));

        session.close();
        driver.close();
        return ResponseEntity.ok(response);
    }

    @PostMapping(value = "/insert/purchases", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertPurchases(@RequestParam("min") Integer min, @RequestParam("max") Integer max, @RequestParam("batchSize") Integer batchSize) {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
        Session session = driver.session();
        Response response = new Response();

        int nbPerson;
        int nbProducts;

        try (Transaction tx = session.beginTransaction()) {
            Result result = tx.run("MATCH (n:Person) RETURN count(n) as count");
            nbPerson = result.single().get(0).asInt();
            result = tx.run("MATCH (n:Product) RETURN count(n) as count");
            nbProducts = result.single().get(0).asInt();
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        long elapsedTime = 0;
        int count = batchSize / ((max - min) / 2);
        int k = 0;

        Map<Integer, Integer> test = new HashMap<>();

        Map<String, Object> params = new HashMap<>();
        List<Map<String, Object>> maps = new ArrayList<>();
        for (int personId = 0; personId < nbPerson; personId++) {
            k++;

            int productNumber = rand.nextInt(max - min + 1) + min;
            for (int y = 0; y < productNumber; y++) {
                int productId = rand.nextInt(nbProducts);
                if (test.containsKey(productId)) {
                    test.put(productId, test.get(productId) + 1);
                } else {
                    test.put(productId, 1);
                }

                maps.add(Map.of(
                        "person", personId,
                        "purchase", productId
                ));
            }

            if (k == count) {
                params.put("props", maps);
                try (Transaction tx = session.beginTransaction()) {
                    String query = "UNWIND $props AS properties " +
                            " MATCH (a:Person), (b:Product) " +
                            " WHERE a.id = properties.person AND b.id = properties.purchase " +
                            " CREATE (a)-[r:Purchase]->(b)";

                    maps = new ArrayList<>();
                    long begin = System.currentTimeMillis();
                    tx.run(query, params);
                    tx.commit();
                    long end = System.currentTimeMillis();
                    elapsedTime += (end - begin);
                    System.out.print("Inserting " + (((double) batchSize / (double) (end - begin)) * 1000) + " nodes per second.\n");
                }
                k = 0;
            }
            response.setExecutionTime((elapsedTime) / Math.pow(10, 3));
        }

        session.close();
        driver.close();
        return ResponseEntity.ok(response);
    }

    @PostMapping(value = "/insert/friends", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertFriends(@RequestParam("min") Integer min, @RequestParam("max") Integer max, @RequestParam("batchSize") Integer batchSize) {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
        Session session = driver.session();
        Response response = new Response();

        long nbPerson;
        try (Transaction tx = session.beginTransaction()) {
            Result result = tx.run("MATCH (n:Person) RETURN count(n) as count");
            nbPerson = result.single().get(0).asLong();
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        Random rand = new Random();

        long elapsedTime = 0;
        int count = batchSize / ((max - min) / 2);
        int k = 0;

        Map<String, Object> params = new HashMap<>();
        List<Map<String, Object>> maps = new ArrayList<>();
        for (int followed = 0; followed < nbPerson; followed++) {
            k++;
            int friendNumber = rand.nextInt(max + 1 - min) + min;
            for (int y = 0; y < friendNumber; y++) {
                int following = rand.nextInt(Math.toIntExact(nbPerson));
                while (followed == following) {
                    following = rand.nextInt(Math.toIntExact(nbPerson));
                }
                maps.add(Map.of(
                        "followed", followed,
                        "following", following
                ));
            }

            if (k == count) {
                params.put("props", maps);
                try (Transaction tx = session.beginTransaction()) {
                    String query = "UNWIND $props AS properties " +
                            " MATCH (a:Person), (b:Person) " +
                            " WHERE a.id = properties.followed AND b.id = properties.following " +
                            " CREATE (a)-[r:Follow]->(b)";

                    maps = new ArrayList<>();
                    long begin = System.currentTimeMillis();
                    tx.run(query, params);
                    tx.commit();
                    long end = System.currentTimeMillis();
                    elapsedTime += (end - begin);
                    System.out.print("Inserting " + (((double) batchSize / (double) (end - begin)) * 1000) + " nodes per second.\n");

                }
                k = 0;
            }

            response.setExecutionTime((elapsedTime) / Math.pow(10, 3));
        }

        session.close();
        driver.close();
        return ResponseEntity.ok(response);
    }

    @PostMapping(value = "/insert/products", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertProducts(@RequestParam("number") Integer nbProducts) {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
        Session session = driver.session();
        Response response = new Response();

        long elapsedTime = 0;

        Map<String, Object> params = new HashMap<>();
        List<Map<String, Object>> maps = new ArrayList<>();
        int k = 0;
        for (int i = 0; i < (nbProducts / 1000); i++) {
            int count = 1000;

            for (int j = 0; j < count; j++) {
                maps.add(Map.of(
                        "name", RandomStringUtils.randomAlphabetic(8),
                        "id", k
                ));
                k++;
            }


            params.put("props", maps);
            try (Transaction tx = session.beginTransaction()) {
                String query = "UNWIND $props AS properties " +
                        " CREATE (n:Product)" +
                        " SET n = properties";

                maps = new ArrayList<>();
                long begin = System.currentTimeMillis();
                tx.run(query, params);
                tx.commit();
                long end = System.currentTimeMillis();
                elapsedTime += (end - begin);
                System.out.print("Inserting " + (double) count / ((double) (end - begin) / count) + " nodes per second.\n");
            }
        }

        response.setExecutionTime((elapsedTime) / Math.pow(10, 3));

        session.close();
        driver.close();
        return ResponseEntity.ok(response);
    }

    @PostMapping(value = "/reset", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> reset() {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
        Session session = driver.session();
        Response response = new Response();

        long startTime = System.currentTimeMillis();
        try (Transaction tx = session.beginTransaction()) {
            tx.run("call apoc.periodic.iterate(\"MATCH (n) return n\", \"DETACH DELETE n\", {batchSize:1000}) " +
                    "yield batches, total return batches, total");
            tx.commit();
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }
        long stopTime = System.currentTimeMillis();

        try (Transaction tx = session.beginTransaction()) {
            tx.run("CREATE INDEX index_person_id IF NOT EXISTS FOR (n:Person) ON (n.id)");
            tx.run("CREATE INDEX index_product_id IF NOT EXISTS FOR (n:Product) ON (n.id)");
            tx.commit();
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        response.setExecutionTime((stopTime - startTime) / Math.pow(10, 3));

        session.close();
        driver.close();
        return ResponseEntity.ok(response);
    }


    @PostMapping(value = "/get/products-follow", produces = "application/json; charset=utf-8")
    public ResponseEntity<ProductsFollowResponse> getProductFollow(@RequestParam("level") Integer level, @RequestParam("person") Integer idPerson, @RequestParam("product") Optional<Integer> productId, @RequestParam("onlyFirstPersonProducts") Optional<Boolean> onlyFirstPersonProducts) {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
        Session session = driver.session();

        int maxResultRow = 100;
        ProductsFollowResponse response = new ProductsFollowResponse();
        List<ProductsPurchases> list = new ArrayList<>();

        try (Transaction tx = session.beginTransaction()) {
            String query;
            String productFilter = productId.isPresent() ? String.format("{id: %d}", productId.get()) : "";
            if (onlyFirstPersonProducts.isPresent() && onlyFirstPersonProducts.get().equals(Boolean.TRUE)) {
                query = String.format("MATCH (:Person {id: %d})-[:Purchase]->(p:Product) " +
                        "WITH collect(p) as products " +
                        "MATCH (:Person {id: %d})<-[:Follow *1..%d]-(p:Person) " +
                        "WITH DISTINCT p, products as products " +
                        "MATCH (p)-[:Purchase]->(n:Product %s) " +
                        "WHERE n IN products " +
                        "RETURN n.name, n.id, COUNT(*) " +
                        "ORDER BY COUNT(*) DESC", idPerson, idPerson, level, productFilter);
            } else {
                query = String.format("MATCH (:Person {id: %d})<-[:Follow *1..%d]-(p:Person) " +
                        "WITH DISTINCT p " +
                        "MATCH (p)-[:Purchase]->(n:Product %s) " +
                        "RETURN n.name, n.id, COUNT(*) " +
                        "ORDER BY COUNT(*) DESC", idPerson, level, productFilter);
            }

            long startTime = System.nanoTime();
            Result result = tx.run(query);
            long stopTime = System.nanoTime();
            response.setExecutionTime((stopTime - startTime) / Math.pow(10, 6));

            int countResultRow = 0;
            while (result.hasNext() && countResultRow <= maxResultRow) {
                Record row = result.next();
                ProductsPurchases p = new ProductsPurchases(row.get(0).asString(), row.get(1).asInt(), row.get(2).asInt());
                list.add(p);
                countResultRow++;
            }
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        response.setProductsPurchases(list);
        return ResponseEntity.ok(response);
    }
}
