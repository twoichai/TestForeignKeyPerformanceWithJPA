package com.example.testforeignkeyperformance2;

import com.example.testforeignkeyperformance2.dto.Test1;
import com.example.testforeignkeyperformance2.dto.Test2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;


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

    private void initializeDb( ) {
        int numOfTest1 = 300_000;
        int numOfTest2 = 400_000;
        List<Test1> test1Values = generateTest1(numOfTest1).toList();
        List<Test2> test2Values = generateTest2(numOfTest1, numOfTest2).toList();

        jdbcTemplate.update("DELETE FROM test_2");
        jdbcTemplate.update("DELETE FROM test_1");

        jdbcTemplate.batchUpdate("""
                        INSERT INTO test_1 (id, name)
                        VALUES (?, ?)""",
                test1Values, 100, (ps, test1) -> {
                    ps.setInt(1, test1.getId());
                    ps.setString(2, test1.getName());
                });
        jdbcTemplate.batchUpdate("""
                        INSERT INTO test_2 (id, name, test_1_id)
                        VALUES (?, ?, ?)""",
                test2Values, 100, (ps, test2) -> {
                    ps.setInt(1, test2.getId());
                    ps.setString(2, test2.getName());
                    ps.setInt(3, test2.getTest1Id());
                });


        int iterations = 10;
        testApproach(iterations, "Upsert with foreign key rewrite", test2Values, this::upsertWithForeignKeyRewrite);
        testApproach(iterations, "Upsert", test2Values, this::upsert);
        testApproach(iterations, "Rewrite", test2Values, this::rewrite);
    }

    private void testApproach(
            int iterations,
            String approachName,
            List<Test2> test2Values,
            Consumer<List<Test2>> approach
    ) {
        double averageExecutionTime = IntStream.rangeClosed(1, iterations)
                .mapToLong(iteration -> {
                    List<Test2> updatedTest2Values = updateTest2(iteration, test2Values).toList();
                    long startTimeMillis = System.currentTimeMillis();
                    approach.accept(updatedTest2Values);
                    long endTimeMills = System.currentTimeMillis();

                    return endTimeMills - startTimeMillis;
                })
                .average()
                .orElseThrow();

        System.out.printf("Execution time (in millis): %f, Approach: %s%n", averageExecutionTime, approachName);
    }

    private void upsertWithForeignKeyRewrite(List<Test2> test2Values) {
        jdbcTemplate.batchUpdate("""
                        INSERT INTO test_2 (id, name, test_1_id)
                        VALUES (?, ?, ?)
                        ON CONFLICT (id) DO
                        UPDATE SET name = ?, test_1_id = ?""",
                test2Values, 100, (ps, test2) -> {
                    ps.setInt(1, test2.getId());
                    ps.setString(2, test2.getName());
                    ps.setInt(3, test2.getTest1Id());
                    ps.setString(4, test2.getName());
                    ps.setInt(5, test2.getTest1Id());
                });
    }

    private void upsert(List<Test2> test2Values) {
        jdbcTemplate.batchUpdate("""
                        INSERT INTO test_2 (id, name, test_1_id)
                        VALUES (?, ?, ?)
                        ON CONFLICT (id) DO
                        UPDATE SET name = ?""",
                test2Values, 100, (ps, test2) -> {
                    ps.setInt(1, test2.getId());
                    ps.setString(2, test2.getName());
                    ps.setInt(3, test2.getTest1Id());
                    ps.setString(4, test2.getName());
                });
    }

    private void rewrite(List<Test2> test2Values) {
        jdbcTemplate.update("DELETE FROM test_2");
        jdbcTemplate.batchUpdate("""
                        INSERT INTO test_2 (id, name, test_1_id)
                        VALUES (?, ?, ?)""",
                test2Values, 100, (ps, test2) -> {
                    ps.setInt(1, test2.getId());
                    ps.setString(2, test2.getName());
                    ps.setInt(3, test2.getTest1Id());
                });
    }

    private Stream<Test2> updateTest2(int iteration, List<Test2> rawValues) {
        return rawValues.stream()
                .peek(test2 -> test2.setName("Updated_" + iteration + "_" + test2.getId()));
    }

    private Stream<Test1> generateTest1(int numOfTest1) {
        return IntStream.rangeClosed(1, numOfTest1)
                .mapToObj(id -> {
                    Test1 test1 = new Test1();
                    test1.setId(id);
                    test1.setName("Test1_" + id);
                    return test1;
                });
    }

    private Stream<Test2> generateTest2(int numOfTest1, int numOfTest2) {
        Random random = new Random();
        return IntStream.rangeClosed(1, numOfTest2)
                .mapToObj(id -> {
                    Test2 test2 = new Test2();
                    test2.setId(id);
                    test2.setName("Test2_" + id);
                    test2.setTest1Id(random.nextInt(numOfTest1) + 1);
                    return test2;
                });
    }
}
