package com.ipi.batch.csvimport;

import com.ipi.batch.dto.CommuneCSV;
import com.ipi.batch.model.Commune;
import org.apache.commons.text.WordUtils;
import org.springframework.batch.item.ItemProcessor;

public class CommuneCSVItemProcessor implements ItemProcessor<CommuneCSV, Commune> {
    @Override
    public Commune process(CommuneCSV item) throws Exception {
        Commune commune = new Commune();
        commune.setCodeInsee(item.getCodeInsee());
        commune.setCodePostal(item.getCodePostal());
        //Majuscule première lettre de chaque terme
        String nomCommune = WordUtils.capitalizeFully(item.getNom());
        //Proprification du nom
        nomCommune = nomCommune.replaceAll("^L ", "L'");
        nomCommune = nomCommune.replaceAll(" L ", " L'");
        nomCommune = nomCommune.replaceAll("^D ", "D'");
        nomCommune = nomCommune.replaceAll(" D ", " D'");
        nomCommune = nomCommune.replaceAll("^St ", "Saint ");
        nomCommune = nomCommune.replaceAll(" St ", " Saint ");
        nomCommune = nomCommune.replaceAll("^Ste ", "Sainte ");
        nomCommune = nomCommune.replaceAll(" Sainte ", " Sainte ");
        commune.setNom(nomCommune);
        //Latitude/Longitude
        String[] coordonnees = item.getCoordonneesGps().split(",");
        commune.setLatitude(Double.valueOf(coordonnees[0]));
        commune.setLongitude(Double.valueOf(coordonnees[1]));
        return commune;
    }
}
