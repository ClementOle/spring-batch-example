package com.ipi.batch.dto;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CommuneCSV {

    private String codeInsee;

    private String nom;

    private String codePostal;

    private String ligne5;

    private String libelleAcheminement;

    private String coordonneesGps;


}
