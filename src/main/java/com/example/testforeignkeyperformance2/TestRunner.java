package com.example.testforeignkeyperformance2;

import com.example.testforeignkeyperformance2.dto.Object1;
import com.example.testforeignkeyperformance2.dto.Object2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Component
public class TestRunner implements CommandLineRunner {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public TestRunner(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void run(String... args) throws Exception {
        initializeDb();
    }

    private void initializeDb() {
        int numOfObject1 = 10000;
        int numOfObject2 = 10000;
        List<Object1> object1Values = generateObject1(numOfObject1).toList();
        List<Object2> object2Values = generateObject2(numOfObject1, numOfObject2).toList();

        jdbcTemplate.update("DELETE FROM test_2");
        jdbcTemplate.update("DELETE FROM test_1");

        jdbcTemplate.batchUpdate("""
                        INSERT INTO test_1 (id, name)
                        VALUES (?, ?)""",
                object1Values, 100, (ps, object1) -> {
                    ps.setInt(1, object1.getId());
                    ps.setString(2, object1.getName());
                });
        jdbcTemplate.batchUpdate("""
                        INSERT INTO test_2 (id, name, test_1_id)
                        VALUES (?, ?, ?)""",
                object2Values, 100, (ps, object2) -> {
                    ps.setInt(1, object2.getId());
                    ps.setString(2, object2.getName());
                    ps.setInt(3, object2.getObject1Id());
                });


        int iterations = 10;
        testApproach(iterations, "Upsert with foreign key rewrite", object2Values, this::upsertWithForeignKeyRewrite);
        testApproach(iterations, "Upsert", object2Values, this::upsert);
        testApproach(iterations, "Rewrite", object2Values, this::rewrite);
    }

    private void testApproach(
            int iterations,
            String approachName,
            List<Object2> object2Values,
            Consumer<List<Object2>> approach
    ) {
        double averageExecutionTime = IntStream.rangeClosed(1, iterations)
                .mapToLong(iteration -> {
                    List<Object2> updatedObject2Values = updateObject2(iteration, object2Values).toList();
                    long startTimeMillis = System.currentTimeMillis();
                    approach.accept(updatedObject2Values);
                    long endTimeMills = System.currentTimeMillis();

                    return endTimeMills - startTimeMillis;
                })
                .average()
                .orElseThrow();

        System.out.printf("Execution time (in millis): %f, Approach: %s%n", averageExecutionTime, approachName);
    }

    private void upsertWithForeignKeyRewrite(List<Object2> object2Values) {
        jdbcTemplate.batchUpdate("""
                        INSERT INTO test_2 (id, name, test_1_id)
                        VALUES (?, ?, ?)
                        ON CONFLICT (id) DO
                        UPDATE SET name = ?, test_1_id = ?""",
                object2Values, 100, (ps, object2) -> {
                    ps.setInt(1, object2.getId());
                    ps.setString(2, object2.getName());
                    ps.setInt(3, object2.getObject1Id());
                    ps.setString(4, object2.getName());
                    ps.setInt(5, object2.getObject1Id());
                });
    }

    private void upsert(List<Object2> object2Values) {
        jdbcTemplate.batchUpdate("""
                        INSERT INTO test_2 (id, name, test_1_id)
                        VALUES (?, ?, ?)
                        ON CONFLICT (id) DO
                        UPDATE SET name = ?""",
                object2Values, 100, (ps, object2) -> {
                    ps.setInt(1, object2.getId());
                    ps.setString(2, object2.getName());
                    ps.setInt(3, object2.getObject1Id());
                    ps.setString(4, object2.getName());
                });
    }

    private void rewrite(List<Object2> object2Values) {
        jdbcTemplate.update("DELETE FROM test_2");
        jdbcTemplate.batchUpdate("""
                        INSERT INTO test_2 (id, name, test_1_id)
                        VALUES (?, ?, ?)""",
                object2Values, 100, (ps, object2) -> {
                    ps.setInt(1, object2.getId());
                    ps.setString(2, object2.getName());
                    ps.setInt(3, object2.getObject1Id());
                });
    }

    private Stream<Object2> updateObject2(int iteration, List<Object2> rawValues) {
        return rawValues.stream()
                .peek(object2 -> object2.setName("Updated_" + iteration + "_" + object2.getId()));
    }

    private Stream<Object1> generateObject1(int numOfObject1) {
        return IntStream.rangeClosed(1, numOfObject1)
                .mapToObj(id -> {
                    Object1 object1 = new Object1();
                    object1.setId(id);
                    object1.setName("Object1_" + id);
                    return object1;
                });
    }

    private Stream<Object2> generateObject2(int numOfObject1, int numOfObject2) {
        Random random = new Random();
        return IntStream.rangeClosed(1, numOfObject2)
                .mapToObj(id -> {
                    Object2 object2 = new Object2();
                    object2.setId(id);
                    object2.setName("Object2_" + id);
                    object2.setObject1Id(random.nextInt(numOfObject1) + 1);
                    return object2;
                });
    }
}
