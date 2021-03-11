package com.ipi.batch.csvimport;

import com.ipi.batch.dto.CommuneCSV;
import com.ipi.batch.model.Commune;
import org.springframework.batch.item.ItemProcessor;

public class CommuneCSVItemProcessor implements ItemProcessor<CommuneCSV, Commune> {

    @Override
    public Commune process(CommuneCSV item) throws Exception {
        return null;
    }
}
