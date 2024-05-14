package com.example.testforeignkeyperformance2.repo;

import com.example.testforeignkeyperformance2.dto.Test1H;
import org.springframework.data.jpa.repository.JpaRepository;

public interface Test1HRepo extends JpaRepository<Test1H, Long> {
}
