/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.tabular.iceberg.connect.data;

import static java.util.stream.Collectors.toSet;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT;

import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.common.DynMethods.BoundMethod;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utilities {

  private static final Logger LOG = LoggerFactory.getLogger(Utilities.class.getName());
  private static final List<String> HADOOP_CONF_FILES =
      ImmutableList.of("core-site.xml", "hdfs-site.xml", "hive-site.xml");

  public static Catalog loadCatalog(IcebergSinkConfig config) {
    return CatalogUtil.buildIcebergCatalog(
        config.catalogName(), config.catalogProps(), loadHadoopConfig(config));
  }

  // use reflection here to avoid requiring Hadoop as a dependency
  private static Object loadHadoopConfig(IcebergSinkConfig config) {
    Class<?> configClass = dynamicallyLoad("org.apache.hadoop.hdfs.HdfsConfiguration");
    if (configClass == null) {
      configClass = dynamicallyLoad("org.apache.hadoop.conf.Configuration");
    }

    if (configClass == null) {
      LOG.info("Hadoop not found on classpath, not creating Hadoop config");
      return null;
    }

    try {
      Object result = configClass.getDeclaredConstructor().newInstance();
      BoundMethod addResourceMethod =
          DynMethods.builder("addResource").impl(configClass, URL.class).build(result);
      BoundMethod setMethod =
          DynMethods.builder("set").impl(configClass, String.class, String.class).build(result);

      //  load any config files in the specified config directory
      String hadoopConfDir = config.hadoopConfDir();
      if (hadoopConfDir != null) {
        HADOOP_CONF_FILES.forEach(
            confFile -> {
              Path path = Paths.get(hadoopConfDir, confFile);
              if (Files.exists(path)) {
                try {
                  addResourceMethod.invoke(path.toUri().toURL());
                } catch (IOException e) {
                  LOG.warn("Error adding Hadoop resource {}, resource was not added", path, e);
                }
              }
            });
      }

      // set any Hadoop properties specified in the sink config
      config.hadoopProps().forEach(setMethod::invoke);

      LOG.info("Hadoop config initialized: {}", configClass.getName());
      return result;
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      LOG.warn(
          "Hadoop found on classpath but could not create config, proceeding without config", e);
    }
    return null;
  }

  public static Object extractFromRecordValue(Object recordValue, String fieldName) {
    String[] fields = fieldName.split("\\.");
    if (recordValue instanceof Struct) {
      return getValueFromStruct((Struct) recordValue, fields, 0);
    } else if (recordValue instanceof Map) {
      return getValueFromMap((Map<?, ?>) recordValue, fields, 0);
    } else {
      throw new UnsupportedOperationException(
          "Cannot extract value from type: " + recordValue.getClass().getName());
    }
  }

  public static Long extractTxIdFromRecordValue(Object recordValue, String fieldName) {
    Object txId = extractFromRecordValue(recordValue, fieldName);
    if (txId instanceof Number) {
      return ((Number) txId).longValue();
    } else {
      return null;
    }
  }

  private static Object getValueFromStruct(Struct struct, String[] fields, int idx) {
    Preconditions.checkArgument(idx < fields.length, "Invalid field index");
    Object value = struct.get(fields[idx]);
    if (value == null || idx == fields.length - 1) {
      return value;
    }

    Preconditions.checkState(value instanceof Struct, "Expected a struct type");
    return getValueFromStruct((Struct) value, fields, idx + 1);
  }

  private static Object getValueFromMap(Map<?, ?> map, String[] fields, int idx) {
    Preconditions.checkArgument(idx < fields.length, "Invalid field index");
    Object value = map.get(fields[idx]);
    if (value == null || idx == fields.length - 1) {
      return value;
    }

    Preconditions.checkState(value instanceof Map, "Expected a map type");
    return getValueFromMap((Map<?, ?>) value, fields, idx + 1);
  }

  public static TaskWriter<Record> createTableWriter(
      Table table, String tableName, IcebergSinkConfig config) {
    Map<String, String> tableProps = Maps.newHashMap(table.properties());
    tableProps.putAll(config.writeProps());

    String formatStr = tableProps.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.valueOf(formatStr.toUpperCase());

    long targetFileSize =
        PropertyUtil.propertyAsLong(
            tableProps, WRITE_TARGET_FILE_SIZE_BYTES, WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();

    // override the identifier fields if the config is set
    List<String> idCols = config.tableConfig(tableName).idColumns();
    if (!idCols.isEmpty()) {
      identifierFieldIds =
          idCols.stream()
              .map(colName -> table.schema().findField(colName).fieldId())
              .collect(toSet());
    }

    FileAppenderFactory<Record> appenderFactory;
    if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
      appenderFactory =
          new GenericAppenderFactory(table.schema(), table.spec(), null, null, null)
              .setAll(tableProps);
    } else {
      appenderFactory =
          new GenericAppenderFactory(
                  table.schema(),
                  table.spec(),
                  Ints.toArray(identifierFieldIds),
                  TypeUtil.select(table.schema(), Sets.newHashSet(identifierFieldIds)),
                  null)
              .setAll(tableProps);
    }

    // (partition ID + task ID + operation ID) must be unique
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(format)
            .build();

    TaskWriter<Record> writer;
    if (table.spec().isUnpartitioned()) {
      if (config.tablesCdcField() == null && !config.upsertModeEnabled()) {
        writer =
            new UnpartitionedWriter<>(
                table.spec(), format, appenderFactory, fileFactory, table.io(), targetFileSize);
      } else {
        writer =
            new UnpartitionedDeltaWriter(
                table.spec(),
                format,
                appenderFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                table.schema(),
                identifierFieldIds,
                config.upsertModeEnabled());
      }
    } else {
      if (config.tablesCdcField() == null && !config.upsertModeEnabled()) {
        writer =
            new PartitionedAppendWriter(
                table.spec(),
                format,
                appenderFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                table.schema());
      } else {
        writer =
            new PartitionedDeltaWriter(
                table.spec(),
                format,
                appenderFactory,
                fileFactory,
                table.io(),
                targetFileSize,
                table.schema(),
                identifierFieldIds,
                config.upsertModeEnabled());
      }
    }
    return writer;
  }

  /**
   * Dynamically load hive/hadoop configs to avoid packaging them with the distribution. Gradle
   * strips hadoop from the classpath which will cause a NoClassDefFoundError to be thrown when
   * using the version without Hive, so intercept that exception to maintain the underlying
   * DynClass.builder.impl(...).orNull() behavior.
   */
  private static Class<?> dynamicallyLoad(String className) {
    Class<?> configClass;
    try {
      configClass = DynClasses.builder().impl(className).orNull().build();
    } catch (NoClassDefFoundError e) {
      configClass = null;
    }
    return configClass;
  }

  public static <C> void close(C closeable) {
    if (closeable != null) {
      if (closeable instanceof AutoCloseable) {
        try {
          ((AutoCloseable) closeable).close();
        } catch (Exception e) {
          LOG.warn(
              "An error occurred while trying to close {} instance, ignoring...",
              closeable.getClass().getSimpleName(),
              e);
        }
      }
    }
  }

  public static Long calculateTxIdValidThrough(Map<?, Long> highestTxIdPerPartition) {
    if (highestTxIdPerPartition.isEmpty()) {
      return 0L;
    }

    // Find the minimum value in the map, as it represents the highest transaction ID
    // that is common across all partitions
    long minValue = Collections.min(highestTxIdPerPartition.values());

    // Subtract 1 from the minimum value to get the last guaranteed completed transaction ID
    // If minValue is 1, then there are no completed transactions, so return 0
    return minValue > 1 ? minValue - 1 : 0;
  }

  public static Long getMaxTxId(Map<?, Long> highestTxIdPerPartition) {
    return highestTxIdPerPartition.values().stream().max(Long::compareTo).orElse(0L);
  }

  private Utilities() {}
}
