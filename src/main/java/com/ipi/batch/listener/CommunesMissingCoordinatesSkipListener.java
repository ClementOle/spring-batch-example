package com.ipi.batch.listener;

import com.ipi.batch.model.Commune;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

public class CommunesMissingCoordinatesSkipListener implements SkipListener<Commune, Commune> {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Override
    public void onSkipInRead(Throwable t) {
        logger.warn("Skip in Read => " + t.getMessage());
    }

    @Override
    public void onSkipInWrite(Commune item, Throwable t) {
        logger.warn("Skip in Write => " + item.toString() + ", " + t.getMessage());
    }

    @Override
    public void onSkipInProcess(Commune item, Throwable t) {
        logger.warn("Skip in Process => " + item.toString() + ", " + t.getMessage());
    }
}
