package com.example.testforeignkeyperformance2.dto;

import jakarta.persistence.*;

@Table(name = "test_2h")
@Entity
public class Test2H {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String name;

    private Long test1Id;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getTest1Id() {
        return test1Id;
    }

    public void setTest1Id(Long test1Id) {
        this.test1Id = test1Id;
    }
}
