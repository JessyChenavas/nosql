package com.tp.sql.controllers;

import com.tp.sql.entity.DatabaseStatus;
import com.tp.sql.entity.ProductsFollowResponse;
import com.tp.sql.entity.ProductsPurchases;
import com.tp.sql.entity.Response;
import com.tp.sql.mapper.ProductsFollowRowMapper;
import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

@RestController
@RequestMapping("/api/sql")
public class SQLController {

    @Autowired
    JdbcTemplate jdbcTemplate;

    public SQLController() {
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

    @PostMapping(value = "/reset", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> createTables() {
        Response response = new Response();
        try {
            long startTime = System.currentTimeMillis();
            jdbcTemplate.execute(deletePersonTable);
            jdbcTemplate.execute(createPersonTable);

            jdbcTemplate.execute(deleteFriendTable);
            jdbcTemplate.execute(createFriendTable);

            jdbcTemplate.execute(deleteProductTable);
            jdbcTemplate.execute(createProductTable);

            jdbcTemplate.execute(deletePurchaseTable);
            jdbcTemplate.execute(createPurchaseTable);

            /*jdbcTemplate.execute("DROP INDEX index_person_id ON Person");
            jdbcTemplate.execute("DROP INDEX index_product_id ON Product");*/

            /*jdbcTemplate.execute("CREATE INDEX index_person_id ON Person (id);");
            jdbcTemplate.execute("CREATE INDEX index_product_id ON Product (id);");*/

            long stopTime = System.currentTimeMillis();
            response.setExecutionTime((stopTime - startTime) / Math.pow(10, 3));
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(response);
    }

    @GetMapping(value = "/status", produces = "application/json; charset=utf-8")
    public ResponseEntity<DatabaseStatus> status() {
        DatabaseStatus status = new DatabaseStatus();
        try {
            Long nbPerson = jdbcTemplate.queryForObject("SELECT Count(*) FROM Person", Long.class);
            status.setPersonsNumber(nbPerson);
            Long nbProducts = jdbcTemplate.queryForObject("SELECT Count(*) FROM Product", Long.class);
            status.setProductsNumber(nbProducts);
            Long nbFriends = jdbcTemplate.queryForObject("SELECT Count(*) FROM Friend", Long.class);
            status.setFriendsNumber(nbFriends);
            Long nbPurchases = jdbcTemplate.queryForObject("SELECT Count(*) FROM Purchase", Long.class);
            status.setPurchasesNumber(nbPurchases);
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(status);
    }

    @PostMapping(value = "/insert/persons", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertPersons(@RequestParam("number") Integer number, @RequestParam("batchSize") Integer batchSize) {
        Response response = new Response();
        try {
            Random rand = new Random();
            long elapsedTime = 0;
            for (int i = 0; i < (number / batchSize); i++) {
                List<Object[]> list = new ArrayList<>();

                for (int y = 0; y < batchSize; y++) {
                    String firstName = firstNames.get(rand.nextInt(firstNames.size()));
                    String lastName = lastNames.get(rand.nextInt(lastNames.size()));
                    list.add(new Object[]{firstName, lastName});
                }

                long begin = System.currentTimeMillis();
                jdbcTemplate.batchUpdate("INSERT INTO Person(first_name, last_name) VALUES (?,?)", list);
                long end = System.currentTimeMillis();
                elapsedTime += (end - begin);
                System.out.print("Inserting " + (((double) batchSize / (double) (end - begin)) * 1000) + " nodes per s.\n");
            }
            response.setExecutionTime((elapsedTime) / Math.pow(10, 3));
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(response);
    }

    @PostMapping(value = "/insert/products", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertProducts(@RequestParam("number") Integer number) {
        Response response = new Response();
        try {
            List<Object[]> list = new ArrayList<>();
            for (int i = 0; i < number; i++) {
                list.add(new Object[]{RandomStringUtils.randomAlphabetic(8)});
            }

            long startTime = System.currentTimeMillis();
            // Uses JdbcTemplate's batchUpdate operation to bulk load data
            jdbcTemplate.batchUpdate("INSERT INTO Product(name) VALUES (?)", list);
            long stopTime = System.currentTimeMillis();
            response.setExecutionTime((stopTime - startTime) / Math.pow(10, 3));
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(response);
    }

    @PostMapping(value = "/insert/friends", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertFriends(@RequestParam("min") Integer min, @RequestParam("max") Integer max, @RequestParam("batchSize") Integer batchSize) {
        Response response = new Response();
        try {
            Long nbPerson = jdbcTemplate.queryForObject("SELECT Count(*) FROM Person", Long.class);

            if (nbPerson == null) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST, "Error");
            }

            Random rand = new Random();
            long elapsedTime = 0;
            int count = batchSize / ((max - min) / 2);
            int k = 0;

            List<Object[]> list = new ArrayList<>();
            for (int followed = 0; followed <= nbPerson; followed++) {
                int friendNumber = rand.nextInt(max + 1 - min) + min;
                for (int y = 0; y < friendNumber; y++) {
                    int following = rand.nextInt(Math.toIntExact(nbPerson));
                    while (followed == following) {
                        following = rand.nextInt(Math.toIntExact(nbPerson));
                    }
                    list.add(new Object[]{followed, following});
                }

                if (k == count) {
                    System.out.println("Insertion d'un batch");
                    System.out.println("Id Personne actuelle: " + followed);
                    long begin = System.currentTimeMillis();
                    jdbcTemplate.batchUpdate("INSERT INTO Friend(id_friend, id_friend_of) VALUES (?, ?)", list);
                    long end = System.currentTimeMillis();
                    elapsedTime += (end - begin);
                    list = new ArrayList<>();
                    k = 0;
                    System.out.print("Inserting " + (((double) batchSize / (double) (end - begin)) * 1000) + " nodes per s.\n");
                }

                k++;
            }
            response.setExecutionTime((elapsedTime) / Math.pow(10, 3));
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }

        return ResponseEntity.ok(response);
    }


    @PostMapping(value = "/insert/purchases", produces = "application/json; charset=utf-8")
    public ResponseEntity<Response> insertPurchases(@RequestParam("min") Integer min, @RequestParam("max") Integer max, @RequestParam("batchSize") Integer batchSize) {
        Response response = new Response();
        try {
            Integer nbPersons = jdbcTemplate.queryForObject("SELECT Count(*) FROM Person", Integer.class);
            Integer nbProducts = jdbcTemplate.queryForObject("SELECT Count(*) FROM Product", Integer.class);

            if (nbProducts == null || nbPersons == null) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST, "Error");
            }

            Random rand = new Random();
            long elapsedTime = 0;
            int count = batchSize / ((max - min) / 2);
            int k = 0;

            List<Object[]> list = new ArrayList<>();
            for (int personId = 0; personId <= nbPersons; personId++) {
                int productNumber = rand.nextInt(max - min + 1) + min;
                for (int y = 0; y < productNumber; y++) {
                    int productId = rand.nextInt(nbProducts);
                    list.add(new Object[]{personId, productId});
                }

                if (k == count) {
                    long begin = System.currentTimeMillis();
                    jdbcTemplate.batchUpdate("INSERT INTO Purchase(id_person, id_product) VALUES (?, ?)", list);
                    long end = System.currentTimeMillis();
                    elapsedTime += (end - begin);
                    list = new ArrayList<>();
                    k = 0;
                    System.out.print("Inserting " + (((double) batchSize / (double) (end - begin)) * 1000) + " nodes per s.\n");
                }
                k++;
            }
            response.setExecutionTime((elapsedTime) / Math.pow(10, 3));
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }
        return ResponseEntity.ok(response);
    }

    @PostMapping(value = "/get/products-follow", produces = "application/json; charset=utf-8")
    public ResponseEntity<ProductsFollowResponse> getProductFollow(@RequestParam("level") Integer level, @RequestParam("person") Integer idPerson, @RequestParam("product") Optional<Integer> productId,
                                                                   @RequestParam("onlyFirstPersonProducts") Optional<Boolean> onlyFirstPersonProducts) {

        List<ProductsPurchases> listP = new ArrayList<>();
        ProductsFollowResponse response = new ProductsFollowResponse();
        try {

            String query = String.format("WITH RECURSIVE cte AS (" +
                    "  SELECT *, 1 level " +
                    "  FROM friend" +
                    "  WHERE id_friend = %d " +
                    "  UNION ALL " +
                    "  SELECT f.*, level + 1 " +
                    "  FROM cte c INNER JOIN friend f " +
                    "  ON f.id_friend = c.id_friend_of " +
                    "  WHERE f.id_friend_of <> 1 AND level < %d) " +
                    "  SELECT p.name AS name, p.id as id, COUNT(p.name) AS nb " +
                    "     FROM product p " +
                    "     JOIN purchase pu ON pu.id_product = p.id " +
                    " WHERE pu.id_person IN" +
                    "  (SELECT DISTINCT id_friend_of FROM cte)", idPerson, level);

            if (onlyFirstPersonProducts.isPresent() && onlyFirstPersonProducts.get().equals(Boolean.TRUE)) {
                query = String.format(query + " AND pu.id_product IN (SELECT DISTINCT id_product FROM purchase pu WHERE pu.id_person = %d)", idPerson);
            }

            if (productId.isPresent()) {
                query = String.format(query + " AND p.id = %d ", productId.get());
            }
            query = query + " GROUP BY name ORDER BY nb DESC;";

            long startTime = System.currentTimeMillis();
            System.out.println(query);

            jdbcTemplate.setMaxRows(10);
            listP = jdbcTemplate.query(
                    query, new ProductsFollowRowMapper());
            long stopTime = System.currentTimeMillis();

            response.setExecutionTime((stopTime - startTime) / Math.pow(10, 3));
            response.setProductsPurchases(listP);
        } catch (Exception e) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "Error", e);
        }
        return ResponseEntity.ok(response);
    }

    // Everything below: Unused
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
}

