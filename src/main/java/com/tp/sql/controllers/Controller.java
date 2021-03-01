package com.tp.sql.controllers;

import com.tp.sql.entity.ProductsFollowResponse;
import com.tp.sql.entity.ProductsPurchases;
import com.tp.sql.entity.Response;
import com.tp.sql.entity.neo4j.Person;
import com.tp.sql.mapper.ProductsFollowRowMapper;
import com.tp.sql.repository.PersonRepository;
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

import java.util.*;

@RestController
@RequestMapping("/api")
public class Controller {

    @Autowired
    JdbcTemplate jdbcTemplate;


    private final Driver driver;

    private final PersonRepository personRepository;

    public Controller(Driver driver, PersonRepository personRepository) {
        this.driver = driver;
        this.personRepository = personRepository;
    }

    private String deleteFriendTable = "DROP TABLE IF EXISTS Friend";
    private String createFriendTable = "CREATE TABLE Friend(" +
            "       id INT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
            "       id_friend INT NOT NULL," +
            "       id_friend_of INT NOT NULL," +
            "       FOREIGN KEY (id_friend) references Person(id) ON DELETE RESTRICT," +
            "       FOREIGN KEY (id_friend_of) references Person(id) ON DELETE RESTRICT);";

    private String deletePersonTable = "DROP TABLE IF EXISTS Person";
    private String createPersonTable = "CREATE TABLE Person(" +
            "       id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
            "       first_name VARCHAR(255), " +
            "       last_name VARCHAR(255))";

    private String deleteProductTable = "DROP TABLE IF EXISTS Product";
    private String createProductTable = "CREATE TABLE Product(" +
            "       id INT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
            "       name VARCHAR(255))";

    private String deletePurchaseTable = "DROP TABLE IF EXISTS Purchase";
    private String createPurchaseTable = "CREATE TABLE Purchase(" +
            "       id INT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
            "       id_person INT NOT NULL," +
            "       id_product INT NOT NULL," +
            "       FOREIGN KEY (id_person) references Person(id) ON DELETE RESTRICT," +
            "       FOREIGN KEY (id_product) references Product(id) ON DELETE RESTRICT);";

    private List<String> firstNames = Arrays.asList("Virginie", "Bastien", "Jessy", "Dorian", "Guillaume", "Alexis", "Bertrand");
    private List<String> lastNames = Arrays.asList("Sansone", "Girard-Blanc", "Chenavas", "Dignac", "Lagay", "Pouzeratte", "Masse");
    private List<String> products = Arrays.asList("Frigo", "Canapé", "Tabouret", "Lot de poêles", "Lit", "Nourriture", "Gel douche", "Concombre");

    @PostMapping(value = "/create-table", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> createTables() {
        Response response = new Response();
        try {
            long startTime = System.nanoTime();
            jdbcTemplate.execute(deletePersonTable);
            jdbcTemplate.execute(createPersonTable);

            jdbcTemplate.execute(deleteFriendTable);
            jdbcTemplate.execute(createFriendTable);

            jdbcTemplate.execute(deleteProductTable);
            jdbcTemplate.execute(createProductTable);

            jdbcTemplate.execute(deletePurchaseTable);
            jdbcTemplate.execute(createPurchaseTable);
            long stopTime = System.nanoTime();
            response.setExecution_time((stopTime - startTime) / Math.pow(10, 6));
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(response);
    }

    @PostMapping(value = "/insert/persons", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertPersons(@RequestParam("number") Integer number) {
        Response response = new Response();
        try {
            Random rand = new Random();
            List<Object[]> list = new ArrayList<>();
            for (int i = 0; i < number; i++) {
                String firstName = firstNames.get(rand.nextInt(firstNames.size()));
                String lastName = lastNames.get(rand.nextInt(lastNames.size()));
                list.add(new Object[]{firstName, lastName});
            }

            long startTime = System.nanoTime();
            // Uses JdbcTemplate's batchUpdate operation to bulk load data
            jdbcTemplate.batchUpdate("INSERT INTO Person(first_name, last_name) VALUES (?,?)", list);
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
            Random rand = new Random();
            List<Object[]> list = new ArrayList<>();
            products.forEach(product -> {
                list.add(new Object[]{product});
            });

            long startTime = System.nanoTime();
            // Uses JdbcTemplate's batchUpdate operation to bulk load data
            jdbcTemplate.batchUpdate("INSERT INTO Product(name) VALUES (?)", list);
            long stopTime = System.nanoTime();
            response.setExecution_time((stopTime - startTime) / Math.pow(10, 6));
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(response);
    }


    @PostMapping(value = "/insert/friends", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertFriends(@RequestParam("min") Integer min, @RequestParam("max") Integer max) {
        Response response = new Response();
        try {
            Long nbPerson = jdbcTemplate.queryForObject("SELECT Count(*) FROM Person", Long.class);

            if (nbPerson == null) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST, "Error");
            }

            Random rand = new Random();
            List<Object[]> list = new ArrayList<>();
            for (int friendId = 0; friendId < nbPerson; friendId++) {

                int friendNumber = rand.nextInt(max + 1 - min) + min;
                for (int y = 0; y < friendNumber; y++) {
                    int friendOfId = rand.nextInt(Math.toIntExact(nbPerson));
                    while (friendId == friendOfId) {
                        friendOfId = rand.nextInt(Math.toIntExact(nbPerson));
                    }
                    list.add(new Object[]{friendId, friendOfId});
                }
            }

            long startTime = System.nanoTime();
            // Uses JdbcTemplate's batchUpdate operation to bulk load data
            jdbcTemplate.batchUpdate("INSERT INTO Friend(id_friend, id_friend_of) VALUES (?, ?)", list);
            long stopTime = System.nanoTime();
            response.setExecution_time((stopTime - startTime) / Math.pow(10, 6));
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(response);
    }


    @PostMapping(value = "/insert/purchases", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertPurchases(@RequestParam("min") Integer min, @RequestParam("max") Integer max) {
        Response response = new Response();
        try {
            Long nbPersons = jdbcTemplate.queryForObject("SELECT Count(*) FROM Person", Long.class);
            Long nbProducts = jdbcTemplate.queryForObject("SELECT Count(*) FROM Product", Long.class);
            // System.out.println(nb);
            if (nbProducts == null || nbPersons == null) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST, "Error");
            }

            Random rand = new Random();
            List<Object[]> list = new ArrayList<>();
            for (int personId = 0; personId < nbPersons; personId++) {
                int productNumber = rand.nextInt(max + 1 - min) + min;
                for (int y = 0; y < productNumber; y++) {
                    int productId = rand.nextInt(Math.toIntExact(nbProducts));
                    list.add(new Object[]{personId, productId});
                }
            }

            long startTime = System.nanoTime();
            // Uses JdbcTemplate's batchUpdate operation to bulk load data
            jdbcTemplate.batchUpdate("INSERT INTO Purchase(id_person, id_product) VALUES (?, ?)", list);
            long stopTime = System.nanoTime();
            response.setExecution_time((stopTime - startTime) / Math.pow(10, 6));
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(response);
    }


    @PostMapping(value = "/delete/persons", produces = "application/json; charset=utf-8")
    public ResponseEntity<Boolean> deletePersons() {
        try {
            jdbcTemplate.execute(deletePersonTable);
            jdbcTemplate.execute(createFriendTable);
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(true);
    }

    @PostMapping(value = "/delete/products", produces = "application/json; charset=utf-8")
    public ResponseEntity<Boolean> deleteProducts() {
        try {
            jdbcTemplate.execute(deleteProductTable);
            jdbcTemplate.execute(createProductTable);
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(true);
    }


    @PostMapping(value = "/delete/friends", produces = "application/json; charset=utf-8")
    public ResponseEntity<Boolean> deleteFriends() {
        try {
            jdbcTemplate.execute(deleteFriendTable);
            jdbcTemplate.execute(createFriendTable);
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(true);
    }


    @PostMapping(value = "/delete/purchases", produces = "application/json; charset=utf-8")
    public ResponseEntity<Boolean> deletePurchases() {
        try {
            jdbcTemplate.execute(deletePurchaseTable);
            jdbcTemplate.execute(createPurchaseTable);
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(true);
    }


    @PostMapping(value = "/get/products-follow", produces = "application/json; charset=utf-8")
    public ResponseEntity<ProductsFollowResponse> getProductFollow(@RequestParam("level") Integer level, @RequestParam("person") Integer idPerson, @RequestParam("product") Optional<Integer> productId) {

        List<ProductsPurchases> listP = new ArrayList<>();
        ProductsFollowResponse response = new ProductsFollowResponse();
        try {

            StringBuilder builder = new StringBuilder();

            // Build CTE recursif avec l'id de la personne pour laquelle on effectue la recherche
            builder.append("WITH RECURSIVE cte AS (" +
                    "  SELECT *, 1 level " +
                    "  FROM friend" +
                    "  WHERE id_friend = ");
            builder.append(idPerson);
            builder.append(" UNION ALL" +
                    "  SELECT f.*, level + 1" +
                    "  FROM cte c INNER JOIN friend f" +
                    "  ON f.id_friend = c.id_friend_of" +
                    "  WHERE f.id_friend_of <> 1 AND level < ");
            builder.append(level);
            builder.append(")");

            // Ajoute le select pour récuperer le nom et compte des produits achetés
            builder.append("SELECT p.name AS name, COUNT(p.name) AS nb" +
                    "  FROM product p" +
                    "  JOIN purchase pu ON pu.id_product = p.id ");

            // Rajoute une clause pour ne récupere qu'un produit spécifique si besoin
            if (productId.isPresent()) {
                builder.append("WHERE p.id = ");
                builder.append(productId.get());
                builder.append(" AND ");
            } else {
                builder.append("WHERE ");
            }
            builder.append("pu.id_person IN" +
                    "  (SELECT DISTINCT id_friend_of FROM cte)" +
                    "  GROUP BY name;");


            long startTime = System.nanoTime();
            listP = jdbcTemplate.query(
                    builder.toString(), new ProductsFollowRowMapper());
            long stopTime = System.nanoTime();

            response.setExecution_time((stopTime - startTime) / Math.pow(10, 6));
            response.setProductsPurchases(listP);
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }
        return ResponseEntity.ok(response);
    }




}

