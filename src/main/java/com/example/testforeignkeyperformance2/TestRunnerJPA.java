package com.example.testforeignkeyperformance2;

import com.example.testforeignkeyperformance2.dto.Object1H;
import com.example.testforeignkeyperformance2.dto.Object2H;
import com.example.testforeignkeyperformance2.repo.Object1HRepo;
import com.example.testforeignkeyperformance2.repo.Object2HRepo;
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
    private Object1HRepo object1HRepo;
    @Autowired
    private Object2HRepo object2HRepo;

    @Override
    public void run(String... args) throws Exception {
        initializeDb();
    }

    private void initializeDb() {
        int numOfObject1H = 1;
        int numOfObject2H = 1;

        List<Object1H> object1HValues = generateObject1H(numOfObject1H).collect(Collectors.toList());
        List<Object2H> object2HValues = generateObject2H(numOfObject1H, numOfObject2H).collect(Collectors.toList());

        object1HRepo.deleteAllInBatch();
        object2HRepo.deleteAllInBatch();

        object1HRepo.saveAll(object1HValues);
        object1HRepo.flush(); // Ensure all Object1H entities are persisted before inserting Object2H entities
        object2HRepo.saveAll(object2HValues);
        object2HRepo.flush();

        int iterations = 10;
        testApproach(iterations, "Upsert with foreign key rewrite", object2HValues, this::upsertWithForeignKeyRewrite);
        testApproach(iterations, "Upsert", object2HValues, this::upsert);
        testApproach(iterations, "Rewrite", object2HValues, this::rewrite);
    }

    private void testApproach(
            int iterations,
            String approachName,
            List<Object2H> object2HValues,
            Consumer<List<Object2H>> approach
    ) {
        double averageExecutionTime = IntStream.rangeClosed(1, iterations)
                .mapToLong(iteration -> {
                    List<Object2H> updatedObject2HValues = updateObject2H(iteration, object2HValues).collect(Collectors.toList());
                    long startTimeMillis = System.currentTimeMillis();
                    approach.accept(updatedObject2HValues);
                    long endTimeMillis = System.currentTimeMillis();

                    return endTimeMillis - startTimeMillis;
                })
                .average()
                .orElseThrow();

        System.out.printf("Execution time (in millis): %f, Approach with JPA: %s%n", averageExecutionTime, approachName);
    }

    private void upsertWithForeignKeyRewrite(List<Object2H> object2HValues) {
        object2HValues.forEach(object2H -> {
            Object2H existing = object2HRepo.findById(object2H.getId()).orElse(null);
            if (existing != null) {
                existing.setName(object2H.getName());
                existing.setObject1HId(object2H.getObject1HId());
                object2HRepo.save(existing);
            } else {
                object2HRepo.save(object2H);
            }
        });
    }

    private void upsert(List<Object2H> object2HValues) {
        object2HValues.forEach(object2H -> {
            Object2H existing = object2HRepo.findById(object2H.getId()).orElse(null);
            if (existing != null) {
                existing.setName(object2H.getName());
                object2HRepo.save(existing);
            } else {
                object2HRepo.save(object2H);
            }
        });
    }

    private void rewrite(List<Object2H> object2HValues) {
        object2HRepo.deleteAllInBatch();
        object2HRepo.saveAll(object2HValues);
    }

    private Stream<Object2H> updateObject2H(int iteration, List<Object2H> rawValues) {
        return rawValues.stream()
                .peek(object2H -> object2H.setName("Updated_" + iteration + "_" + object2H.getId()));
    }

    private Stream<Object1H> generateObject1H(int numOfObject1H) {
        return IntStream.rangeClosed(1, numOfObject1H)
                .mapToObj(id -> {
                    Object1H object1H = new Object1H();
                    object1H.setId((long) id);
                    object1H.setName("Object1H_" + id);
                    return object1H;
                });
    }

    private Stream<Object2H> generateObject2H(int numOfObject1H, int numOfObject2H) {
        Random random = new Random();
        return IntStream.rangeClosed(1, numOfObject2H)
                .mapToObj(id -> {
                    Object2H object2H = new Object2H();
                    object2H.setId((long) id); // Assuming ID is a Long type
                    object2H.setName("Object2H_" + id);
                    object2H.setObject1HId((long) (random.nextInt(numOfObject1H) + 1)); // Generates a random Object1H ID within valid range
                    return object2H;
                });
    }
}