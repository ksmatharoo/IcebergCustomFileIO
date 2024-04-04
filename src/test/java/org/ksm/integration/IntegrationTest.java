package org.ksm.integration;

import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CustomFileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class IntegrationTest {

    @Test
    public void testIntegration() throws Exception {

        String userDirectory = System.getProperty("user.dir");
        String dir = "/data";
        String wareHousePath = userDirectory + dir;
        String thriftURl = "thrift://172.18.0.5:9083";

        String dbName = "default";
        String tableName = "test_table";
        TableIdentifier tableIdentifier = TableIdentifier.of(dbName, tableName);

        String oldBasePath = wareHousePath + "/" + tableName;
        String newBasePath = wareHousePath + "/moved_" + tableName;

        SparkSession spark = Utils.getSparkSession(wareHousePath, thriftURl);

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("warehouse", wareHousePath);
        properties.put(CatalogProperties.URI, thriftURl);
        properties.put(CatalogProperties.CATALOG_IMPL, HiveCatalog.class.getName());
        properties.put(CatalogProperties.FILE_IO_IMPL, CustomFileIO.class.getName());

        //Utils.injectFileIOConfigs(spark,oldBasePath,newBasePath);
        HiveCatalog hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(spark.sparkContext().hadoopConfiguration());
        hiveCatalog.initialize("hive", properties);

        //drop table if exist
        Utils.dropTable(spark, tableIdentifier.toString());

        // create and load table in append mode
        Dataset<Row> rowDataset = Utils.readCSVFileWithoutDate(spark, "");
        IcebergTableCreation tableCreation = new IcebergTableCreation(hiveCatalog);
        tableCreation.createTableFromDataset(rowDataset,
                Arrays.asList("hire_date"),
                tableIdentifier,
                wareHousePath);
        rowDataset.writeTo(tableIdentifier.toString()).append();

        //del dir if exist and move table to other location
        Utils.deleteDirectory(spark, newBasePath);
        boolean moved = Utils.moveDirectory(spark, oldBasePath, newBasePath);
        if (!moved) {
            throw new RuntimeException("Table not moved to new location");
        }

        //try to query moved table should throw error
        try {
            spark.sql("select * from " + tableIdentifier).show();
        } catch (Exception e) {
            log.error("caught exception ", e);
        }

        // stop spark session,
        spark.stop();

        //create spark again, set FileIOConf and query table
        {
            SparkSession sparkSession = Utils.getSparkSession(wareHousePath, thriftURl);
            //set hadoop conf
            Utils.injectFileIOConfigs(sparkSession, oldBasePath, newBasePath);
            hiveCatalog = new HiveCatalog();
            //set hadoop conf for hive catalog
            hiveCatalog.setConf(sparkSession.sparkContext().hadoopConfiguration());
            hiveCatalog.initialize("hive", properties);
            sparkSession.sql("select * from " + tableIdentifier).show();

            Utils.dropTable(sparkSession, tableIdentifier.toString());
            Utils.deleteDirectory(sparkSession, newBasePath);

        }
        log.info("end");
    }
}

