/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.tabular.iceberg.connect.data2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.OutputFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.apache.iceberg.TableProperties.*;

/**
 * @author Ismail Simsek
 */
public class IcebergUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);
  protected static final DateTimeFormatter dtFormater = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);



  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier, Schema schema) {

    if (!((SupportsNamespaces) icebergCatalog).namespaceExists(tableIdentifier.namespace())) {
      ((SupportsNamespaces) icebergCatalog).createNamespace(tableIdentifier.namespace());
      LOGGER.warn("Created namespace:'{}'", tableIdentifier.namespace());
    }
    return icebergCatalog.createTable(tableIdentifier, schema);
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
                                         Schema schema, String writeFormat) {

    LOGGER.warn("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schema,
        schema.identifierFieldNames());

    if (!((SupportsNamespaces) icebergCatalog).namespaceExists(tableIdentifier.namespace())) {
      ((SupportsNamespaces) icebergCatalog).createNamespace(tableIdentifier.namespace());
      LOGGER.warn("Created namespace:'{}'", tableIdentifier.namespace());
    }

    return icebergCatalog.buildTable(tableIdentifier, schema)
        .withProperty(FORMAT_VERSION, "2")
        .withProperty(DEFAULT_FILE_FORMAT, writeFormat.toLowerCase(Locale.ENGLISH))
        .withSortOrder(IcebergUtil.getIdentifierFieldsAsSortOrder(schema))
        .create();
  }

  private static SortOrder getIdentifierFieldsAsSortOrder(Schema schema) {
    SortOrder.Builder sob = SortOrder.builderFor(schema);
    for (String fieldName : schema.identifierFieldNames()) {
      sob = sob.asc(fieldName);
    }

    return sob.build();
  }

  public static Optional<Table> loadIcebergTable(Catalog icebergCatalog, TableIdentifier tableId) {
    try {
      Table table = icebergCatalog.loadTable(tableId);
      return Optional.of(table);
    } catch (NoSuchTableException e) {
      LOGGER.warn("Table not found: {}", tableId.toString());
      return Optional.empty();
    }
  }

  public static FileFormat getTableFileFormat(Table icebergTable) {
    String formatAsString = icebergTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
  }

  public static GenericAppenderFactory getTableAppender(Table icebergTable) {
    return new GenericAppenderFactory(
        icebergTable.schema(),
        icebergTable.spec(),
        Ints.toArray(icebergTable.schema().identifierFieldIds()),
        icebergTable.schema(),
        null);
  }


  public static OutputFileFactory getTableOutputFileFactory(Table icebergTable, FileFormat format) {
    return OutputFileFactory.builderFor(icebergTable,
            IcebergUtil.partitionId(), 1L)
        .defaultSpec(icebergTable.spec())
        .operationId(java.util.UUID.randomUUID().toString())
        .format(format)
        .build();
  }

  public static int partitionId() {
    return Integer.parseInt(dtFormater.format(Instant.now()));
  }

}
