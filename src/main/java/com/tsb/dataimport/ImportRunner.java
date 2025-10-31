package com.tsb.dataimport;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.tsb.test.TestDataPreparator;

@Component
public class ImportRunner implements CommandLineRunner {

	private final TestDataPreparator preparator;
    private final DataTransferService service;
    private final DataTransferProperties props;
    
    public ImportRunner(TestDataPreparator preparator, DataTransferService service, DataTransferProperties props) {
        this.preparator = preparator;
        this.service = service;
        this.props = props;
    }

    
    @Override
    public void run(String... args) throws Exception {
    	preparator.prepareIfNeeded();
        service.runAll();
    }
}