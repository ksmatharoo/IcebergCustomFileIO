package org.apache.iceberg;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.*;
import org.apache.iceberg.util.SerializableSupplier;

import java.util.Map;
import java.util.function.Function;

@Slf4j
public class CustomFileIO implements FileIO, HadoopConfigurable, SupportsPrefixOperations {

    HadoopFileIO fileIO;
    String basePath;
    String[] oldBaseUris;

    public CustomFileIO(Configuration conf) {
        this(new SerializableConfiguration(conf)::get);
    }

    public CustomFileIO(SerializableSupplier<Configuration> conf) {
        this.basePath = conf.get().get("fs.baseuri", "");
        this.oldBaseUris = conf.get().get("fs.oldbaseuri", "").split(",");
        this.fileIO = new HadoopFileIO(conf.get());
    }

    public CustomFileIO() {
    }

    @Override
    public void initialize(Map<String, String> properties) {
        fileIO.initialize(properties);
    }

    @Override
    public Map<String, String> properties() {
        return fileIO.properties();
    }

    @Override
    public void serializeConfWith(Function<Configuration, SerializableSupplier<Configuration>> function) {
        this.fileIO.serializeConfWith(function);
    }

    @Override
    public void setConf(Configuration conf) {
        this.basePath = conf.get("fs.baseuri", "");
        this.oldBaseUris = conf.get("fs.oldbaseuri", "").split(",");
        this.fileIO = new HadoopFileIO(conf);
    }

    @Override
    public Configuration getConf() {
        return fileIO.conf();
    }

    @Override
    public Iterable<FileInfo> listPrefix(String path) {
        return fileIO.listPrefix(updatedPathIfRequired(path));
    }

    @Override
    public void deletePrefix(String path) {
        fileIO.deletePrefix(updatedPathIfRequired(path));
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return fileIO.newOutputFile(updatedPathIfRequired(path));
    }

    @Override
    public void deleteFile(String location) {
        fileIO.deleteFile(updatedPathIfRequired(location));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        String updatedPath = updatedPathIfRequired(path);
        InputFile inputFile = fileIO.newInputFile(updatedPath, length);
        if (!updatedPath.equals(path)) {
            try {
                FieldUtils.writeField(inputFile, "location", path, true);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return inputFile;
    }

    @Override
    public InputFile newInputFile(String path) {
        String updatedPath = updatedPathIfRequired(path);
        InputFile inputFile = fileIO.newInputFile(updatedPath);
        if (!updatedPath.equals(path)) {
            try {
                FieldUtils.writeField(inputFile, "location", path, true);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        return inputFile;
    }

    private String updatedPathIfRequired(final String location) {
        if (StringUtils.isBlank(basePath) || location.startsWith(basePath)) {
            return location;
        }
        for (String existingPath : oldBaseUris) {
            if (location.startsWith(existingPath)) {
                log.info("{} : path updated to : {} ", existingPath, basePath);
                return location.replace(existingPath, basePath);
            }
        }
        throw new RuntimeException("Not matching with any base path" + location);
    }
}
