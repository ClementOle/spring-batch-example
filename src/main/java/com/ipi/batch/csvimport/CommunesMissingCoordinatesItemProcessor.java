package com.ipi.batch.csvimport;

import com.ipi.batch.dto.CommuneCSV;
import com.ipi.batch.exception.CommuneCSVException;
import com.ipi.batch.model.Commune;
import com.ipi.batch.utils.OpenStreetMapUtils;
import org.springframework.batch.item.ItemProcessor;

import java.util.Map;

public class CommunesMissingCoordinatesItemProcessor implements ItemProcessor<Commune, Commune> {
    @Override
    public Commune process(Commune item) throws Exception {

        validateCommuneCSV(item);

        Map<String, Double> coordinatesOSM = OpenStreetMapUtils.getInstance().getCoordinates(item.getNom() + " " + item.getCodePostal());

        if (coordinatesOSM != null && coordinatesOSM.size() == 2) {
            item.setLongitude(coordinatesOSM.get("lon"));
            item.setLatitude(coordinatesOSM.get("lat"));
            return item;
        }

        return null;

    }

    private void validateCommuneCSV(CommuneCSV item) throws CommuneCSVException {
        //Contrôler Code INSEE 5 chiffres
        if(item.getCodeInsee() != null && !item.getCodeInsee().matches("^[0-9]{5}$")){
            throw new CommuneCSVException("Le code Insee ne contient pas 5 chiffres");
        }
        //Contrôler Code postal 5 chiffres
        if(item.getCodePostal() != null && !item.getCodePostal().matches("^[0-9]{5}$")){
            throw new CommuneCSVException("Le code Postal ne contient pas 5 chiffres");
        }
        //Contrôler nom de la communes lettres en majuscules, espaces, tirets, et apostrophes
        if(item.getNom() != null && !item.getNom().matches("^[A-Z-' ]+$")){
            throw new CommuneCSVException("Le nom de la commune n'est pas composé uniquement de lettres, espaces et tirets");
        }
        //Contrôler les coordonnées GPS
        if(item.getCoordonneesGps() != null && !item.getCoordonneesGps().matches("^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?),\\s*[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$")){
            throw new CommuneCSVException("Le nom de la commune n'est pas composé uniquement de lettres, espaces et tirets");
        }
    }
}
