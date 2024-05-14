package com.example.testforeignkeyperformance2;

import com.example.testforeignkeyperformance2.dto.Test1H;
import com.example.testforeignkeyperformance2.dto.Test2H;
import com.example.testforeignkeyperformance2.repo.Test1HRepo;
import com.example.testforeignkeyperformance2.repo.Test2HRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.function.Consumer;


@Component
public class TestRunnerJPA implements CommandLineRunner {
    @Autowired
    private Test1HRepo test1HRepo;
    @Autowired
    private Test2HRepo test2HRepo;

    @Override
    public void run(String... args) throws Exception {
        initializeDb();
    }

    private void initializeDb() {
        int numOfTest1 = 3_000;
        int numOfTest2 = 4_000;

        List<Test1H> test1Values = generateTest1(numOfTest1).collect(Collectors.toList());
        List<Test2H> test2Values = generateTest2(numOfTest1, numOfTest2).collect(Collectors.toList());

        test1HRepo.deleteAllInBatch();
        test2HRepo.deleteAllInBatch();

        test1HRepo.saveAll(test1Values);
        test1HRepo.flush(); // Ensure all Test1H entities are persisted before inserting Test2H entities
        test2HRepo.saveAll(test2Values);
        test2HRepo.flush();

        int iterations = 10;
        testApproach(iterations, "Upsert with foreign key rewrite", test2Values, this::upsertWithForeignKeyRewrite);
        testApproach(iterations, "Upsert", test2Values, this::upsert);
        testApproach(iterations, "Rewrite", test2Values, this::rewrite);
    }

    private void testApproach(
            int iterations,
            String approachName,
            List<Test2H> test2Values,
            Consumer<List<Test2H>> approach
    ) {
        double averageExecutionTime = IntStream.rangeClosed(1, iterations)
                .mapToLong(iteration -> {
                    List<Test2H> updatedTest2Values = updateTest2(iteration, test2Values).collect(Collectors.toList());
                    long startTimeMillis = System.currentTimeMillis();
                    approach.accept(updatedTest2Values);
                    long endTimeMillis = System.currentTimeMillis();

                    return endTimeMillis - startTimeMillis;
                })
                .average()
                .orElseThrow();

        System.out.printf("Execution time (in millis): %f, Approach with JPA: %s%n", averageExecutionTime, approachName);
    }

    private void upsertWithForeignKeyRewrite(List<Test2H> test2Values) {
        test2Values.forEach(test2 -> {
            Test2H existing = test2HRepo.findById(test2.getId()).orElse(null);
            if (existing != null) {
                existing.setName(test2.getName());
                existing.setTest1Id(test2.getTest1Id());
                test2HRepo.save(existing);
            } else {
                test2HRepo.save(test2);
            }
        });
    }

    private void upsert(List<Test2H> test2Values) {
        test2Values.forEach(test2 -> {
            Test2H existing = test2HRepo.findById(test2.getId()).orElse(null);
            if (existing != null) {
                existing.setName(test2.getName());
                test2HRepo.save(existing);
            } else {
                test2HRepo.save(test2);
            }
        });
    }

    private void rewrite(List<Test2H> test2Values) {
        test2HRepo.deleteAllInBatch();
        test2HRepo.saveAll(test2Values);
    }

    private Stream<Test2H> updateTest2(int iteration, List<Test2H> rawValues) {
        return rawValues.stream()
                .peek(test2 -> test2.setName("Updated_" + iteration + "_" + test2.getId()));
    }

    private Stream<Test1H> generateTest1(int numOfTest1) {
        return IntStream.rangeClosed(1, numOfTest1)
                .mapToObj(id -> {
                    Test1H test1 = new Test1H();
                    test1.setId((long) id);
                    test1.setName("Test1_" + id);
                    return test1;
                });
    }

    private Stream<Test2H> generateTest2(int numOfTest1, int numOfTest2) {
        Random random = new Random();
        return IntStream.rangeClosed(1, numOfTest2)
                .mapToObj(id -> {
                    Test2H test2 = new Test2H();
                    test2.setId((long) id); // Assuming ID is a Long type
                    test2.setName("Test2_" + id);
                    test2.setTest1Id((long) (random.nextInt(numOfTest1) + 1)); // Generates a random Test1 ID within valid range
                    return test2;
                });
    }
}