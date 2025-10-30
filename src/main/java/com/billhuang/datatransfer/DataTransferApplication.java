package com.billhuang.datatransfer;

import com.billhuang.datatransfer.config.DataTransferProperties;
import com.billhuang.datatransfer.service.DataTransferService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import javax.sql.DataSource;

/**
 * Spring Boot application entry. DataSources should be configured in your Spring context
 * (for example application.yml, or test configuration), providing beans:
 *  - mainDataSource (name can be made via @Bean in config)
 *  - lookupDataSource
 *
 * This runner will optionally prepare test tables (when datatransfer.prepare-test-data=true)
 * and then invoke the DataTransferService.
 */
@SpringBootApplication
@EnableConfigurationProperties(DataTransferProperties.class)
public class DataTransferApplication implements CommandLineRunner {

    private final TestDataPreparator preparator;
    private final DataTransferService service;
    private final DataTransferProperties props;

    public DataTransferApplication(TestDataPreparator preparator, DataTransferService service, DataTransferProperties props) {
        this.preparator = preparator;
        this.service = service;
        this.props = props;
    }

    public static void main(String[] args) {
        SpringApplication.run(DataTransferApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        preparator.prepareIfNeeded();
        service.runAll();
    }
}