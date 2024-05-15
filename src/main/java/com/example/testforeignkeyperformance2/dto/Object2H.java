package com.example.testforeignkeyperformance2.dto;

import jakarta.persistence.*;

@Table(name = "test_2h")
@Entity
public class Object2H {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String name;

    private Long object1HId;

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

    public Long getObject1HId() {
        return object1HId;
    }

    public void setObject1HId(Long object1HId) {
        this.object1HId = object1HId;
    }
}
