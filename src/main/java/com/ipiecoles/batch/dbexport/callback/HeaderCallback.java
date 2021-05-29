package com.ipiecoles.batch.dbexport.callback;


import com.ipiecoles.batch.repository.CommuneRepository;
import org.springframework.batch.item.file.FlatFileHeaderCallback;

import java.io.IOException;
import java.io.Writer;

public class HeaderCallback implements FlatFileHeaderCallback {

    private final CommuneRepository communeRepository;

    @Override
    public void writeHeader(Writer writer) throws IOException {
        writer.write("Total codes postaux : " + communeRepository.countDistinctCodePostal());
    }


    public HeaderCallback(CommuneRepository communeRepository) {
        this.communeRepository = communeRepository;
    }
}
