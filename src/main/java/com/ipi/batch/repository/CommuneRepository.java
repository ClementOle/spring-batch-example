package com.ipi.batch.repository;

import com.ipi.batch.model.Commune;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface CommuneRepository extends JpaRepository<Commune, String> {

    @Query("select count(distinct c.codePostal) from Commune c")
    long countDistinctCodePostal();

}
