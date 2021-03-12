package com.ipi.batch.csvimport;

import com.ipi.batch.dto.CommuneCSV;
import com.ipi.batch.exception.CommuneCSVException;
import com.ipi.batch.model.Commune;
import com.ipi.batch.utils.OpenStreetMapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.annotation.AfterProcess;
import org.springframework.batch.core.annotation.BeforeProcess;
import org.springframework.batch.core.annotation.OnProcessError;
import org.springframework.batch.item.ItemProcessor;

import java.util.Map;

public class CommunesMissingCoordinatesItemProcessor implements ItemProcessor<Commune, Commune> {

    @Override
    public Commune process(Commune item) throws Exception {

        Map<String, Double> coordinatesOSM = OpenStreetMapUtils.getInstance().getCoordinates(item.getNom() + " " + item.getCodePostal());

        if (coordinatesOSM != null && coordinatesOSM.size() == 2) {
            item.setLongitude(coordinatesOSM.get("lon"));
            item.setLatitude(coordinatesOSM.get("lat"));
            return item;
        }

        return null;
    }
}
